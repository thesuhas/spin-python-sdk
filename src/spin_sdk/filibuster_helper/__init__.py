import json
import hashlib
import traceback
import re
import os
from os.path import exists

# We're making an assumption here that test files start with test_ (Pytest)
TEST_PREFIX = "test_"

# We're making an assumption here that test files start with test_ (Pytest)
INSTRUMENTATION_PREFIX = "spin_sdk/http"


def should_fail_request_with(request, requests_to_fail):
    print("Request: \n" + str(request))
    print("Requests to fail: \n" + str(requests_to_fail))
    for request_to_fail in requests_to_fail:
        if request['execution_index'] == request_to_fail['execution_index']:
            print("Failing request.")
            return request_to_fail
    print("Not failing request.")
    return None


def load_counterexample(path):
    try:
        f = open(path)
        counterexample = json.load(f)
        print("Counterexample loaded from file.")
        f.close()
    except IOError:
        raise Exception("No counterexample found at {}; aborting.".format(path))
    return counterexample


def get_full_traceback_hash(service_name):
    raw_callsite = None

    for line in traceback.format_stack():
        if TEST_PREFIX not in line and INSTRUMENTATION_PREFIX not in line:
            raw_callsite = line
            break

    cs_search = re.compile("File \"(.*)\", line (.*), in")
    callsite = cs_search.search(raw_callsite)

    callsite_file = callsite.group(1)
    callsite_line = callsite.group(2)
    print("=> callsite_file: " + callsite_file)
    print("=> callsite_line: " + callsite_line)
    # print("Before Formatting: " + '\n'.join(traceback.format_stack()))
    tracebacks = []
    for _traceback in traceback.format_stack():
        raw_quotes = _traceback.split("\"")
        revised_quotes = []
        # Filter the stacktrace only to the filibuster and app code.
        is_filibuster_stacktrace = False
        for quote in raw_quotes:
            # Analyze paths.
            if "/" in quote:
                # Remove information about which python version we are using.
                if "python" in quote:
                    quote = quote.split("python", 1)[1]
                    if "/" in quote and quote[0] != "/":
                        quote = quote.split("/", 1)[1]
                # Remove absolute path information and keep things relative only to filibuster.
                elif "spin_sdk" in quote:
                    quote = quote.split("spin_sdk", 1)[1]
                    is_filibuster_stacktrace = True
            revised_quotes.append(quote)
        if is_filibuster_stacktrace:
            curr_traceback = "\"".join(revised_quotes)
            tracebacks.append(curr_traceback)

    full_traceback = "\n".join(tracebacks)
    #print(f"Stacktrace: {full_traceback}")
    full_traceback_hash = hashlib.md5(full_traceback.encode()).hexdigest()

    return callsite_file, callsite_line, full_traceback_hash


def counterexample_file():
    return os.environ.get('COUNTEREXAMPLE_FILE', '')


def should_load_counterexample_file():
    return exists(counterexample_file())
