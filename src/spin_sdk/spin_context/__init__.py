_GLOBAL_CONTEXT = {}


def set_value(key, value):
    global _GLOBAL_CONTEXT
    prev_value = None
    if key in _GLOBAL_CONTEXT:
        prev_value = _GLOBAL_CONTEXT[key]
    _GLOBAL_CONTEXT[key] = value
    return prev_value


def get_value(key):
    global _GLOBAL_CONTEXT
    if key in _GLOBAL_CONTEXT:
        return _GLOBAL_CONTEXT[key]
    else:
        return None