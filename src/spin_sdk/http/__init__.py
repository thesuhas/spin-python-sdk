"""Module with helpers for wasi http"""

import asyncio
import json
import sys
import os
import hashlib
import re
import traceback

import spin_sdk.server_helper
from spin_sdk.http import poll_loop
from spin_sdk.http.poll_loop import PollLoop, Sink, Stream
from spin_sdk.wit import exports
from spin_sdk.wit.types import Ok, Err
from spin_sdk.wit.imports.types import (
    IncomingResponse, Method, Method_Get, Method_Head, Method_Post, Method_Put, Method_Delete, Method_Connect,
    Method_Options,
    Method_Trace, Method_Patch, Method_Other, IncomingRequest, IncomingBody, ResponseOutparam, OutgoingResponse,
    Fields, Scheme, Scheme_Http, Scheme_Https, Scheme_Other, OutgoingRequest, OutgoingBody
)
from spin_sdk.wit.imports.streams import StreamError_Closed
from dataclasses import dataclass
from collections.abc import MutableMapping
from typing import Optional
from urllib import parse
import importlib

# Filibuster stuff
import functools
import inspect
from threading import Lock
from spin_sdk.spin_context import get_value as _filibuster_global_context_get_value
from spin_sdk.spin_context import set_value as _filibuster_global_context_set_value
from spin_sdk.filibuster_helper import should_fail_request_with, load_counterexample, get_full_traceback_hash, \
    should_load_counterexample_file, counterexample_file
from spin_sdk.vclock import vclock_new, vclock_increment, vclock_tostring, vclock_fromstring, vclock_equals, \
    vclock_merge, vclock_descends
from spin_sdk.execution_index import execution_index_new, execution_index_push, execution_index_pop, \
    execution_index_tostring, execution_index_fromstring
from spin_sdk.nginx_http_special_response import get_response
from spin_sdk.datatypes import TestExecution
from pprint import pprint
from spin_sdk.spin_logger import warning, debug, notice, info
import uuid
import time

# A key to a context variable to avoid creating duplicate spans when instrumenting
# both, Session.request and Session.send, since Session.request calls into Session.send
_FILIBUSTER_SUPPRESS_REQUESTS_INSTRUMENTATION_KEY = "filibuster_suppress_requests_instrumentation"

# We do not want to instrument the instrumentation, so use this key to detect when we
# are inside of a Filibuster instrumentation call to suppress further instrumentation.
_FILIBUSTER_INSTRUMENTATION_KEY = "filibuster_instrumentation"

# Key for the Filibuster vclock in the context.
_FILIBUSTER_VCLOCK_KEY = "filibuster_vclock"

# Key for the Filibuster origin vclock in the context.
_FILIBUSTER_ORIGIN_VCLOCK_KEY = "filibuster_origin_vclock"

# Key for the Filibuster execution index in the context.
_FILIBUSTER_EXECUTION_INDEX_KEY = "filibuster_execution_index"

# Key for the Filibuster request id in the context.
_FILIBUSTER_REQUEST_ID_KEY = "filibuster_request_id"

# Key for Filibuster vclock mapping.
_FILIBUSTER_VCLOCK_BY_REQUEST_KEY = "filibuster_vclock_by_request"
_filibuster_global_context_set_value(_FILIBUSTER_VCLOCK_BY_REQUEST_KEY, {})

# Mutex for vclock and execution_index.
ei_and_vclock_mutex = Lock()

# Last used execution index.
# (this is mutated under the same mutex as the vclock.)
_FILIBUSTER_EI_BY_REQUEST_KEY = "filibuster_execution_indices_by_request"
_filibuster_global_context_set_value(_FILIBUSTER_EI_BY_REQUEST_KEY, {})

if should_load_counterexample_file():
    notice("Counterexample file present!")
    counterexample = load_counterexample(counterexample_file())
    counterexample_test_execution = TestExecution.from_json(counterexample['TestExecution']) if counterexample else None
    print(counterexample_test_execution.failures)
else:
    counterexample = None

filibuster_url = None


def set_filibuster_url(url):
    global filibuster_url
    filibuster_url = url
    notice(f"Filibuster URL set to {filibuster_url} by {get_service_name()}")


def set_service_name(service_name):
    _filibuster_global_context_set_value("service_name", service_name)


def get_service_name():
    return _filibuster_global_context_get_value("service_name")


def filibuster_update_url(filibuster_url):
    return "{}/{}/update".format(filibuster_url, 'filibuster')


def filibuster_create_url(filibuster_url):
    return "{}/{}/create".format(filibuster_url, 'filibuster')


def filibuster_new_test_execution_url(filibuster_url, service_name):
    return "{}/{}/new-test-execution/{}".format(filibuster_url, 'filibuster', service_name)


@dataclass
class Request:
    """An HTTP request"""
    method: str
    uri: str
    headers: MutableMapping[str, str]
    body: Optional[bytes]


@dataclass
class Response:
    """An HTTP response"""
    status: int
    headers: MutableMapping[str, str]
    body: Optional[bytes]


def wrapped_before_request(func):
    @functools.wraps(func)
    def before_request(self, request: IncomingRequest, response_out: ResponseOutparam):
        global filibuster_url
        # if _excluded_urls.url_disabled(flask.request.url):
        #     return

        # Each request needs to maintain some things independently of the global state (e.g.
        # execution_index, vclock) for when we issue multiple requests. Unique request_ids
        # can distinguish one request from another. Generate a new unique request_id if one
        # doesn't already exist (new request), otherwise use the existing one.
        headers = request.headers()
        notice(f"Request headers: {headers.entries()}")
        if headers.has('X-Filibuster-Request-Id') and headers.get('X-Filibuster-Request-Id') is not None:
            request_id = bytes.join(b'', headers.get('X-Filibuster-Request-Id')).decode('utf-8')
            debug("Using old request_id: " + request_id, service_name=get_service_name())
        else:
            request_id = str(uuid.uuid4())
            debug("Using new request_id: " + request_id, service_name=get_service_name())
        _filibuster_global_context_set_value(_FILIBUSTER_REQUEST_ID_KEY, request_id)
        debug("** [FLASK] [" + get_service_name() + "]: request-id attached to context: " + str(
            _filibuster_global_context_get_value(_FILIBUSTER_REQUEST_ID_KEY)), service_name=get_service_name())

        if headers.has('X-Filibuster-Execution-Index') and headers.get('X-Filibuster-Execution-Index') is not None:

            payload = {
                'instrumentation_type': 'request_received',
                'generated_id': str(bytes.join(b'', headers.get('X-Filibuster-Generated-Id')).decode('utf-8')),
                'execution_index': str(bytes.join(b'', headers.get('X-Filibuster-Execution-Index')).decode('utf-8')),
                'target_service_name': get_service_name(),
            }

            # All this is responsible for doing is putting the header execution index into the context
            # so that any requests that are triggered from this have the existing execution index.
            _filibuster_global_context_set_value(_FILIBUSTER_EXECUTION_INDEX_KEY,
                                                 bytes.join(b'', headers.get('X-Filibuster-Execution-Index')).decode('utf-8'))
            debug("** [FLASK] [" + get_service_name() + "]: execution-index attached to context: " + str(
                _filibuster_global_context_get_value(_FILIBUSTER_EXECUTION_INDEX_KEY)), service_name=get_service_name())

            # All this is responsible for doing is putting the header vclock into the context
            # so that any requests that are triggered from this, know to merge the incoming vclock in.
            _filibuster_global_context_set_value(_FILIBUSTER_VCLOCK_KEY, bytes.join(b'', headers.get('X-Filibuster-VClock')).decode('utf-8'))
            debug("** [FLASK] [" + get_service_name() + "]: vclock attached to context: " + str(
                _filibuster_global_context_get_value(_FILIBUSTER_VCLOCK_KEY)), service_name=get_service_name())

            # All this is responsible for doing is putting the header origin vclock into the context
            # so that any requests that are triggered from this, know to merge the incoming vclock in.
            _filibuster_global_context_set_value(_FILIBUSTER_ORIGIN_VCLOCK_KEY,
                                                 bytes.join(b'', headers.get('X-Filibuster-Origin-VClock')).decode('utf-8'))
            debug("** [FLASK] [" + get_service_name() + "]: origin-vclock attached to context: " + str(
                _filibuster_global_context_get_value(_FILIBUSTER_ORIGIN_VCLOCK_KEY)), service_name=get_service_name())

            if not (os.environ.get('DISABLE_SERVER_COMMUNICATION', '')) and counterexample is None:
                try:
                    debug("Setting Filibuster instrumentation key...", service_name=get_service_name())
                    token = _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, True)

                    make_request_and_send('post', filibuster_update_url(filibuster_url), {'Content-Type': 'application/json'}, json.dumps(payload).encode('utf-8'))
                except Exception as e:
                    warning("Exception raised during instrumentation (_record_successful_response)!")
                    print(e, file=sys.stderr)
                finally:
                    debug("Removing instrumentation key for Filibuster.", service_name=get_service_name())
                    # context.detach(token)
                    _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, token)
        # else:
        #     debug("No filibuster execution index present", service_name=get_service_name())

        # If we should delay the request to simulate timeouts, do it.
        if headers.has('X-Filibuster-Forced-Sleep') and headers.get('X-Filibuster-Forced-Sleep') is not None:
            sleep_interval_string = bytes.join(b'', headers.get('X-Filibuster-Forced-Sleep')).decode('utf-8')
            sleep_interval = int(sleep_interval_string)
            debug(get_service_name() + "is simulating sleep of " + str(sleep_interval) + " seconds.", service_name=get_service_name())
            if sleep_interval != 0:
                time.sleep(sleep_interval)
        return func(self, request, response_out)

    return before_request


class IncomingHandler(exports.IncomingHandler):
    """Simplified handler for incoming HTTP requests using blocking, buffered I/O."""

    def handle_request(self, request: Request) -> Response:
        """Handle an incoming HTTP request and return a response or raise an error"""
        raise NotImplementedError

    @wrapped_before_request
    def handle(self, request: IncomingRequest, response_out: ResponseOutparam):
        print(f"Incoming Request Callstack: {inspect.stack()}")
        method = request.method()

        if isinstance(method, Method_Get):
            method_str = "GET"
        elif isinstance(method, Method_Head):
            method_str = "HEAD"
        elif isinstance(method, Method_Post):
            method_str = "POST"
        elif isinstance(method, Method_Put):
            method_str = "PUT"
        elif isinstance(method, Method_Delete):
            method_str = "DELETE"
        elif isinstance(method, Method_Connect):
            method_str = "CONNECT"
        elif isinstance(method, Method_Options):
            method_str = "OPTIONS"
        elif isinstance(method, Method_Trace):
            method_str = "TRACE"
        elif isinstance(method, Method_Patch):
            method_str = "PATCH"
        elif isinstance(method, Method_Other):
            method_str = method.value
        else:
            raise AssertionError

        request_body = request.consume()
        request_stream = request_body.stream()
        body = bytearray()
        while True:
            try:
                body += request_stream.blocking_read(16 * 1024)
            except Err as e:
                if isinstance(e.value, StreamError_Closed):
                    request_stream.__exit__()
                    IncomingBody.finish(request_body)
                    break
                else:
                    raise e

        request_uri = request.path_with_query()
        if request_uri is None:
            uri = "/"
        else:
            uri = request_uri

        try:
            simple_response = self.handle_request(Request(
                method_str,
                uri,
                dict(map(lambda pair: (pair[0], str(pair[1], "utf-8")), request.headers().entries())),
                bytes(body)
            ))
        except:
            traceback.print_exc()

            response = OutgoingResponse(Fields())
            response.set_status_code(500)
            ResponseOutparam.set(response_out, Ok(response))
            return

        if simple_response.headers.get('content-length') is None:
            content_length = len(simple_response.body) if simple_response.body is not None else 0
            simple_response.headers['content-length'] = str(content_length)

        response = OutgoingResponse(Fields.from_list(list(map(
            lambda pair: (pair[0], bytes(pair[1], "utf-8")),
            simple_response.headers.items()
        ))))
        response_body = response.body()
        response.set_status_code(simple_response.status)
        ResponseOutparam.set(response_out, Ok(response))
        response_stream = response_body.write()
        if simple_response.body is not None:
            MAX_BLOCKING_WRITE_SIZE = 4096
            offset = 0
            while offset < len(simple_response.body):
                count = min(len(simple_response.body) - offset, MAX_BLOCKING_WRITE_SIZE)
                response_stream.blocking_write_and_flush(simple_response.body[offset:offset + count])
                offset += count
        response_stream.__exit__()
        OutgoingBody.finish(response_body, None)


async def send_async(request: Request) -> Response:
    match request.method:
        case "GET":
            method: Method = Method_Get()
        case "HEAD":
            method = Method_Head()
        case "POST":
            method = Method_Post()
        case "PUT":
            method = Method_Put()
        case "DELETE":
            method = Method_Delete()
        case "CONNECT":
            method = Method_Connect()
        case "OPTIONS":
            method = Method_Options()
        case "TRACE":
            method = Method_Trace()
        case "PATCH":
            method = Method_Patch()
        case _:
            method = Method_Other(request.method)

    url_parsed = parse.urlparse(request.uri)

    match url_parsed.scheme:
        case "http":
            scheme: Scheme = Scheme_Http()
        case "https":
            scheme = Scheme_Https()
        case "":
            scheme = Scheme_Http()
        case _:
            scheme = Scheme_Other(url_parsed.scheme)

    if request.headers.get('content-length') is None:
        content_length = len(request.body) if request.body is not None else 0
        request.headers['content-length'] = str(content_length)

    headers = list(map(
        lambda pair: (pair[0], bytes(pair[1], "utf-8")),
        request.headers.items()
    ))

    outgoing_request = OutgoingRequest(Fields.from_list(headers))
    outgoing_request.set_method(method)
    outgoing_request.set_scheme(scheme)
    if url_parsed.netloc == '':
        if scheme == "http":
            authority = ":80"
        else:
            authority = ":443"
    else:
        authority = url_parsed.netloc

    outgoing_request.set_authority(authority)

    path_and_query = url_parsed.path
    if url_parsed.query:
        path_and_query += '?' + url_parsed.query
    outgoing_request.set_path_with_query(path_and_query)

    outgoing_body = request.body if request.body is not None else bytearray()
    sink = Sink(outgoing_request.body())
    incoming_response: IncomingResponse = (await asyncio.gather(
        poll_loop.send(outgoing_request),
        send_and_close(sink, outgoing_body)
    ))[0]

    response_body = Stream(incoming_response.consume())
    body = bytearray()
    while True:
        chunk = await response_body.next()
        if chunk is None:
            simple_response = Response(
                incoming_response.status(),
                dict(map(
                    lambda pair: (pair[0], str(pair[1], "utf-8")),
                    incoming_response.headers().entries()
                )),
                bytes(body)
            )
            incoming_response.__exit__()
            return simple_response
        else:
            body += chunk


async def send_and_close(sink: Sink, data: bytes):
    await sink.send(data)
    sink.close()


# Filibuster functions - list
def wrap_request(func):
    @functools.wraps(func)
    def instrumented_request(method, uri, headers, body) -> Response:
        debug("instrumented_send entering; method: " + method + " url: " + uri, service_name=get_service_name())

        def get_or_create_headers():
            # request.headers = (
            #     request.headers
            #     if request.headers is not None
            #     else CaseInsensitiveDict()
            # )
            return headers

        def call_wrapped(additional_headers):
            debug("instrumented_send.call_wrapped entering", service_name=get_service_name())
            request = Request(method, uri, headers, body)
            response = normal_send(request)
            debug("instrumented_send.call_wrapped exiting", service_name=get_service_name())
            return response

        response = _instrumented_requests_call(
            method, uri, call_wrapped, get_or_create_headers
        )

        debug("instrumented_send exiting; method: " + method + " url: " + uri, service_name=get_service_name())
        return response

    return instrumented_request


def wrap_send(func):
    @functools.wraps(func)
    def instrumented_send(request: Request, **kwargs) -> Response:
        debug("instrumented_request entering; method: " + request.method + " url: " + request.uri, service_name=get_service_name())
        pprint(os.environ)

        def get_or_create_headers():
            headers = request.headers
            if headers is None:
                headers = {}
                request.headers = headers

            return headers

        def call_wrapped(additional_headers):
            debug(f"instrumented_request.call_wrapped entering: {additional_headers}", service_name=get_service_name())

            # Merge headers: don't worry about collisions, we're only adding information.
            if request.headers is not None:
                headers = request.headers
                for key in additional_headers:
                    headers[key] = additional_headers[key]
                request.headers = headers
            else:
                request.headers = additional_headers

            response = send(request)
            debug("instrumented_request.call_wrapped exiting", service_name=get_service_name())
            return response

        response = _instrumented_requests_call(
            request.method, request.uri, call_wrapped, get_or_create_headers, **kwargs
        )

        debug("instrumented_request exiting; method: " + request.method + " url: " + request.uri, service_name=get_service_name())
        return response

    return instrumented_send


def _instrumented_requests_call(
        method: str, url: str, call_wrapped, get_or_create_headers, **kwargs
):
    generated_id = None
    has_execution_index = False
    exception = None
    status_code = None
    should_inject_fault = False
    should_abort = True
    should_sleep_interval = 0

    debug("_instrumented_requests_call entering; method: " + method + " url: " + url, service_name=get_service_name())

    # Bail early if we are in nested instrumentation calls.
    debug(f"Filibuster Suppress Instrumentation: {_filibuster_global_context_get_value(_FILIBUSTER_SUPPRESS_REQUESTS_INSTRUMENTATION_KEY)} {_filibuster_global_context_get_value('suppress_instrumentation')}", service_name=get_service_name())
    if _filibuster_global_context_get_value("suppress_instrumentation") or _filibuster_global_context_get_value(
            _FILIBUSTER_SUPPRESS_REQUESTS_INSTRUMENTATION_KEY
    ):
        debug(
            "_instrumented_requests_call returning call_wrapped() because _FILIBUSTER_SUPPRESS_REQUESTS_INSTRUMENTATION_KEY set.", service_name=get_service_name())
        return call_wrapped({})

    vclock = None

    origin_vclock = None

    execution_index = None

    # Record that a call is being made to an external service.
    if not _filibuster_global_context_get_value(_FILIBUSTER_INSTRUMENTATION_KEY):
        if not _filibuster_global_context_get_value("suppress_instrumentation"):
            callsite_file, callsite_line, full_traceback_hash = get_full_traceback_hash(get_service_name())

            debug("", service_name=get_service_name())
            debug("Recording call using Filibuster instrumentation service. ********************", service_name=get_service_name())

            # VClock handling.

            # Figure out if we should reset the node's vector clock, which should happen in between test executions.
            debug("Setting Filibuster instrumentation key...", service_name=get_service_name())
            # token = context.attach(_filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, True))
            token = _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, True)
            response = None
            if not (os.environ.get('DISABLE_SERVER_COMMUNICATION', '')) and counterexample is None:
                response = make_request_and_send('get',
                                                 filibuster_new_test_execution_url(filibuster_url, get_service_name()),
                                                 get_or_create_headers(), '')
                if response is not None:
                    response = json.loads(response.body.decode('utf-8'))
                    # debug(f"Response Body: {response}")

            debug("Removing instrumentation key for Filibuster.", service_name=get_service_name())
            # context.detach(token) Detatch the context
            _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, token)
            reset_local_vclock = False
            if response and ('new-test-execution' in response) and (response['new-test-execution']):
                reset_local_vclock = True

            global ei_and_vclock_mutex
            ei_and_vclock_mutex.acquire()

            request_id_string = _filibuster_global_context_get_value(_FILIBUSTER_REQUEST_ID_KEY)

            if reset_local_vclock:
                # Reset everything, since there is a new test execution.
                debug("New test execution. Resetting vclocks_by_request and execution_indices_by_request.", service_name=get_service_name())

                vclocks_by_request = {request_id_string: vclock_new()}
                _filibuster_global_context_set_value(_FILIBUSTER_VCLOCK_BY_REQUEST_KEY, vclocks_by_request)

                execution_indices_by_request = {request_id_string: execution_index_new()}
                _filibuster_global_context_set_value(_FILIBUSTER_EI_BY_REQUEST_KEY, execution_indices_by_request)

            # Incoming clock from the request that triggered this service to be reached.
            incoming_vclock_string = _filibuster_global_context_get_value(_FILIBUSTER_VCLOCK_KEY)

            # If it's not None, we probably need to merge with our clock, first, since our clock is keeping
            # track of *our* requests from this node.
            if incoming_vclock_string is not None:
                vclocks_by_request = _filibuster_global_context_get_value(_FILIBUSTER_VCLOCK_BY_REQUEST_KEY)
                incoming_vclock = vclock_fromstring(incoming_vclock_string)
                local_vclock = vclocks_by_request.get(request_id_string, vclock_new())
                new_local_vclock = vclock_merge(incoming_vclock, local_vclock)
                vclocks_by_request[request_id_string] = new_local_vclock
                _filibuster_global_context_set_value(_FILIBUSTER_VCLOCK_BY_REQUEST_KEY, vclocks_by_request)

            # Finally, advance the clock to account for this request.
            vclocks_by_request = _filibuster_global_context_get_value(_FILIBUSTER_VCLOCK_BY_REQUEST_KEY)
            local_vclock = vclocks_by_request.get(request_id_string, vclock_new())
            new_local_vclock = vclock_increment(local_vclock, get_service_name())
            vclocks_by_request[request_id_string] = new_local_vclock
            _filibuster_global_context_set_value(_FILIBUSTER_VCLOCK_BY_REQUEST_KEY, vclocks_by_request)

            vclock = new_local_vclock

            notice("clock now: " + str(vclocks_by_request.get(request_id_string, vclock_new())))

            # Maintain the execution index for each request.

            incoming_execution_index_string = _filibuster_global_context_get_value(_FILIBUSTER_EXECUTION_INDEX_KEY)

            if incoming_execution_index_string is not None:
                incoming_execution_index = execution_index_fromstring(incoming_execution_index_string)
            else:
                execution_indices_by_request = _filibuster_global_context_get_value(_FILIBUSTER_EI_BY_REQUEST_KEY)
                incoming_execution_index = execution_indices_by_request.get(request_id_string,
                                                                            execution_index_new())

            if os.environ.get("PRETTY_EXECUTION_INDEXES", ""):
                execution_index_hash = url
            else:
                # TODO: can't include kwargs here, not sure why, i think it's metadata?  anyway, should be blank mostly since
                #       everything should be converted to args by this point.
                #       could also be None?
                # Remove host information. This allows us to run counterexamples across different
                # platforms (local, docker, eks) that use different hosts to resolve networking.
                # I.e. since we want http://0.0.0.0:5000/users (local) and http://users:5000/users
                # (docker) to have the same execution index hash, standardize the url to include
                # only the port and path (5000/users).
                url = url.replace('http://', '')
                if ":" in url:
                    url = url.split(":", 1)[1]
                execution_index_hash = unique_request_hash(
                    [full_traceback_hash, 'requests', method, json.dumps(url)])

            execution_indices_by_request = _filibuster_global_context_get_value(_FILIBUSTER_EI_BY_REQUEST_KEY)
            execution_indices_by_request[request_id_string] = execution_index_push(execution_index_hash,
                                                                                   incoming_execution_index)
            execution_index = execution_indices_by_request[request_id_string]
            _filibuster_global_context_set_value(_FILIBUSTER_EI_BY_REQUEST_KEY, execution_indices_by_request)

            ei_and_vclock_mutex.release()

            # Origin VClock Handling.

            # origin-vclock are used to track the explicit request chain
            # that caused this call to be made: more precise than
            # happens-before and required for the reduction strategy to
            # work.
            #
            # For example, if Service A does 4 requests, in sequence,
            # before making a call to Service B, happens-before can be used
            # to show those four requests happened before the call to
            # Service B.  This is correct: vector/Lamport clock track both
            # program order and the communication between nodes in their encoding.
            #
            # However, for the reduction strategy to work, we need to know
            # precisely *what* call in in Service A triggered the call to
            # Service B (and, recursively if Service B is to make any
            # calls, as well.) This is because the key to the reduction
            # strategy is to remove tests from the execution list where
            # there is no observable difference at the boundary between the
            # two services. Therefore, we need to identify precisely where
            # these boundary points are.
            #

            # This is a clock that's been received through Flask as part of processing the current request.
            # (flask receives context via header and sets into context object; requests reads it.)
            incoming_origin_vclock_string = _filibuster_global_context_get_value(_FILIBUSTER_ORIGIN_VCLOCK_KEY)
            debug("** [REQUESTS] [" + get_service_name() + "]: getting incoming origin vclock string: " + str(
                incoming_origin_vclock_string), service_name=get_service_name())

            # This isn't used in the record_call, but just propagated through the headers in the subsequent request.
            origin_vclock = vclock

            # Record call with the incoming origin clock and advanced clock.
            if incoming_origin_vclock_string is not None:
                incoming_origin_vclock = vclock_fromstring(incoming_origin_vclock_string)
            else:
                incoming_origin_vclock = vclock_new()
            response = _record_call(method, [url], callsite_file, callsite_line, full_traceback_hash, vclock,
                                    incoming_origin_vclock, execution_index_tostring(execution_index), kwargs)

            if response is not None:
                if 'generated_id' in response:
                    generated_id = response['generated_id']

                if 'execution_index' in response:
                    has_execution_index = True

                if 'forced_exception' in response:
                    exception = response['forced_exception']['name']

                    if 'metadata' in response['forced_exception'] and response['forced_exception'][
                        'metadata'] is not None:
                        exception_metadata = response['forced_exception']['metadata']
                        if 'abort' in exception_metadata and exception_metadata['abort'] is not None:
                            should_abort = exception_metadata['abort']
                        if 'sleep' in exception_metadata and exception_metadata['sleep'] is not None:
                            should_sleep_interval = exception_metadata['sleep']

                    should_inject_fault = True

                if 'failure_metadata' in response:
                    if 'return_value' in response['failure_metadata'] and 'status_code' in \
                            response['failure_metadata']['return_value']:
                        status_code = response['failure_metadata']['return_value']['status_code']
                        should_inject_fault = True

            debug("Finished recording call using Filibuster instrumentation service. ***********", service_name=get_service_name())
            debug("", service_name=get_service_name())
        else:
            debug("Instrumentation suppressed, skipping Filibuster instrumentation.", service_name=get_service_name())

    try:
        debug("Setting Filibuster instrumentation key...", service_name=get_service_name())
        # token = context.attach(_filibuster_global_context_set_value(_FILIBUSTER_SUPPRESS_REQUESTS_INSTRUMENTATION_KEY, True))
        token = _filibuster_global_context_set_value(_FILIBUSTER_SUPPRESS_REQUESTS_INSTRUMENTATION_KEY, True)
        if has_execution_index:
            request_id = _filibuster_global_context_get_value("filibuster_request_id")
            if not should_inject_fault:
                # Propagate vclock and origin vclock forward.
                result = call_wrapped(
                    {
                        'X-Filibuster-Generated-Id': str(generated_id),
                        'X-Filibuster-VClock': vclock_tostring(vclock),
                        'X-Filibuster-Origin-VClock': vclock_tostring(origin_vclock),
                        'X-Filibuster-Execution-Index': execution_index_tostring(execution_index),
                        'X-Filibuster-Request-Id': str(request_id)
                    }
                )
            elif should_inject_fault and not should_abort:
                # Propagate vclock and origin vclock forward.
                result = call_wrapped(
                    {
                        'X-Filibuster-Generated-Id': str(generated_id),
                        'X-Filibuster-VClock': vclock_tostring(vclock),
                        'X-Filibuster-Origin-VClock': vclock_tostring(origin_vclock),
                        'X-Filibuster-Execution-Index': execution_index_tostring(execution_index),
                        'X-Filibuster-Forced-Sleep': str(should_sleep_interval),
                        'X-Filibuster-Request-Id': str(request_id)
                    }
                )
            else:
                # Return entirely fake response and do not make request.
                #
                # Since this isn't a real result object, there's some attribute that's
                # being set to None and that's causing -- for these requests -- the opentelemetry
                # to not be able to report this correctly with the following error in the output:
                #
                # "Invalid type NoneType for attribute value.
                # Expected one of ['bool', 'str', 'int', 'float'] or a sequence of those types"
                #
                # I'm going to ignore this for now, because if we reorder the instrumentation
                # so that the opentelemetry is installed *before* the Filibuster instrumentation
                # we should be able to avoid this -- it's because we're returning an invalid
                # object through the opentelemetry instrumentation.
                #
                result = Response
        else:
            result = call_wrapped({})
    except Exception as exc:
        exception = exc
        result = getattr(exc, "response", None)
    finally:
        debug("Removing instrumentation key for Filibuster.", service_name=get_service_name())
        # context.detach(token)
        _filibuster_global_context_set_value(_FILIBUSTER_SUPPRESS_REQUESTS_INSTRUMENTATION_KEY, token)

    # Result was an actual response.
    if isinstance(result, Response) and (exception is None or exception == "None"):
        debug("_instrumented_requests_call got response!", service_name=get_service_name())

        if has_execution_index:
            _update_execution_index()

        if should_inject_fault:
            # If the status code should be something else, change it.
            if status_code is not None:
                result.status = int(status_code)
                # Get the default response for the status code.
                default_response = ''
                if os.environ.get('SET_ERROR_CONTENT', ''):
                    default_response = get_response(status_code)
                result.headers['Content-Type'] = 'text/html'
                result._content = default_response.encode()

        # Notify the filibuster server of the actual response.
        if generated_id is not None:
            _record_successful_response(generated_id, execution_index_tostring(execution_index), vclock,
                                        result)

        # if result.raw and result.raw.version:
        #     version = (str(result.raw.version)[:1] + "." + str(result.raw.version)[:-1])
        #     debug("=> http.version: " + version, service_name=get_service_name())

        debug("=> http.status_code: " + str(result.status), service_name=get_service_name())

    # Result was an exception.
    if exception is not None and exception != "None":
        if isinstance(exception, str):
            exception_class = eval(exception)
            exception = exception_class()
            use_traceback = False
        else:
            if _filibuster_global_context_get_value(_FILIBUSTER_INSTRUMENTATION_KEY):
                # If the Filibuster instrumentation call failed, ignore.  This just means
                # that the test server is unavailable.
                warning("Filibuster instrumentation server unreachable, ignoring...")
                warning("If fault injection is enabled... this indicates that something isn't working properly.")
            else:
                try:
                    exception_info = exception.rsplit('.', 1)
                    m = importlib.import_module(exception_info[0])
                    exception = getattr(m, exception_info[1])
                except Exception:
                    warning("Couldn't get actual exception due to exception parse error.")

            use_traceback = True

        if not _filibuster_global_context_get_value(_FILIBUSTER_INSTRUMENTATION_KEY):
            debug("got exception!", service_name=get_service_name())
            debug("=> exception: " + str(exception), service_name=get_service_name())

            if has_execution_index:
                _update_execution_index()

            # Notify the filibuster server of the actual exception we encountered.
            if generated_id is not None:
                _record_exceptional_response(generated_id, execution_index_tostring(execution_index), vclock,
                                             exception, should_sleep_interval, should_abort)

            if use_traceback:
                raise exception.with_traceback(exception.__traceback__)
            else:
                raise exception

    debug("_instrumented_requests_call exiting; method: " + method + " url: " + url, service_name=get_service_name())
    return result


def _record_call(method, args, callsite_file, callsite_line, full_traceback, vclock, origin_vclock,
                 execution_index, kwargs):
    response = None
    parsed_content = None

    try:
        debug("Setting Filibuster instrumentation key...", service_name=get_service_name())
        # token = context.attach(_filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, True))
        token = _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, True)
        payload = {
            'instrumentation_type': 'invocation',
            'source_service_name': get_service_name(),
            'module': 'requests',
            'method': method,
            'args': args,
            'kwargs': {},
            'callsite_file': get_service_name() + callsite_file,
            'callsite_line': callsite_line,
            'full_traceback': full_traceback,
            'metadata': {},
            'vclock': vclock,
            'origin_vclock': origin_vclock,
            'execution_index': execution_index
        }

        if 'timeout' in kwargs:
            if kwargs['timeout'] is not None:
                debug("=> timeout for call is set to " + str(kwargs['timeout']), service_name=get_service_name())
                payload['metadata']['timeout'] = kwargs['timeout']
            try:
                if args.index('https') >= 0:
                    payload['metadata']['ssl'] = True
            except ValueError:
                pass

        if counterexample is not None and counterexample_test_execution is not None:
            notice("Using counterexample without contacting server.")
            response = should_fail_request_with(payload, counterexample_test_execution.failures)
            if response is None:
                response = {'execution_index': execution_index}
            print(response)
        if os.environ.get('DISABLE_SERVER_COMMUNICATION', ''):
            warning("Server communication disabled.")
        elif counterexample is not None:
            notice("Skipping request, replaying from local counterexample.")
        else:
            response = make_request_and_send('put', filibuster_create_url(filibuster_url), {'Content-Type': 'application/json'}, json.dumps(payload).encode('utf-8'))
    except Exception as e:
        warning("Exception raised (_record_call)!")
        print(e, file=sys.stderr)
        return None
    finally:
        debug("Removing instrumentation key for Filibuster.", service_name=get_service_name())
        # context.detach(token)
        _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, token)

    if isinstance(response, dict):
        parsed_content = response
    elif response is not None:
        try:
            parsed_content = json.loads(response.body.decode('utf-8'))

        except Exception as e:
            warning("Exception raised (_record_call get_json)!")
            print(e, file=sys.stderr)
            return None

    return parsed_content


def _update_execution_index():
    global ei_and_vclock_mutex

    ei_and_vclock_mutex.acquire()

    execution_indices_by_request = _filibuster_global_context_get_value(_FILIBUSTER_EI_BY_REQUEST_KEY)
    request_id_string = _filibuster_global_context_get_value(_FILIBUSTER_REQUEST_ID_KEY)
    if request_id_string in execution_indices_by_request:
        execution_indices_by_request[request_id_string] = execution_index_pop(
            execution_indices_by_request[request_id_string])
        _filibuster_global_context_set_value(_FILIBUSTER_EI_BY_REQUEST_KEY, execution_indices_by_request)

    ei_and_vclock_mutex.release()


def _record_successful_response(generated_id, execution_index, vclock, result):
    # assumes no asynchrony or threads at calling service.

    if not (os.environ.get('DISABLE_SERVER_COMMUNICATION', '')) and counterexample is None:
        try:
            debug("Setting Filibuster instrumentation key...", service_name=get_service_name())
            # token = context.attach()
            token = _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, True)
            return_value = {
                '__class__': str(result.__class__.__name__),
                'status_code': str(result.status),
                'text': hashlib.md5(result.body.decode('utf-8').encode()).hexdigest()
            }
            debug(f'Successfully recorded response: {return_value}', service_name=get_service_name())
            payload = {
                'instrumentation_type': 'invocation_complete',
                'generated_id': generated_id,
                'execution_index': execution_index,
                'vclock': vclock,
                'return_value': return_value
            }
            debug(f"Payload: {payload}", service_name=get_service_name())
            make_request_and_send('post', filibuster_update_url(filibuster_url), {'Content-Type': 'application/json'}, json.dumps(payload).encode('utf-8'))
        except Exception as e:
            warning("Exception raised (_record_successful_response)!")
            print(e, file=sys.stderr)
        finally:
            debug("Removing instrumentation key for Filibuster.", service_name=get_service_name())
            # context.detach(token)
            _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, token)

    return True


def _record_exceptional_response(generated_id, execution_index, vclock, exception, should_sleep_interval,
                                 should_abort):
    # assumes no asynchrony or threads at calling service.

    if not (os.environ.get('DISABLE_SERVER_COMMUNICATION', '')):
        try:
            debug("Setting Filibuster instrumentation key...", service_name=get_service_name())
            # token = context.attach(_filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, True))
            token = _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, True)
            exception_to_string = str(type(exception))
            parsed_exception_string = re.findall(r"'(.*?)'", exception_to_string, re.DOTALL)[0]
            payload = {
                'instrumentation_type': 'invocation_complete',
                'generated_id': generated_id,
                'execution_index': execution_index,
                'vclock': vclock,
                'exception': {
                    'name': parsed_exception_string,
                    'metadata': {

                    }
                }
            }

            if should_sleep_interval > 0:
                payload['exception']['metadata']['sleep'] = should_sleep_interval

            if should_abort is not True:
                payload['exception']['metadata']['abort'] = should_abort

            make_request_and_send('post', filibuster_update_url(filibuster_url), {'Content-Type': 'application/json'}, json.dumps(payload).encode('utf-8'))
        except Exception as e:
            warning("Exception raised (_record_exceptional_response)!")
            print(e, file=sys.stderr)
        finally:
            debug("Removing instrumentation key for Filibuster.", service_name=get_service_name())
            # context.detach(token)
            _filibuster_global_context_set_value(_FILIBUSTER_INSTRUMENTATION_KEY, token)

    return True


# For a given request, return a unique hash that can be used to identify it.
def unique_request_hash(args):
    hash_string = "-".join(args)
    hex_digest = hashlib.md5(hash_string.encode()).hexdigest()
    return hex_digest


@wrap_send
def send(request: Request, **kwargs) -> Response:
    """Send an HTTP request and return a response or raise an error"""
    # print(f"Outgoing Request Callstack: {inspect.stack()}")
    loop = PollLoop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(send_async(request))


@wrap_request
def make_request_and_send(method, uri, headers, body):
    # debug(f"Making Request: {method} {uri} {headers} {body}")
    req = Request(method, uri, headers, body)
    return normal_send(req)


def normal_send(request: Request, **kwargs) -> Response:
    """Send an HTTP request and return a response or raise an error"""
    debug(f"Sending request: {request.method} {request.uri} {request.headers}", get_service_name())
    loop = PollLoop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(send_async(request))


def make_request_and_send_normal(method, uri, headers, body):
    # debug(f"Making Request: {method} {uri} {headers} {body}", service_name=get_service_name())
    req = Request(method, uri, headers, body)
    return normal_send(req)
