from datetime import datetime, timezone


def asserting(x, message=None):
    if message:
        assert x, message
    else:
        assert x
    return x


def fullname(obj):
    if type(obj) != type:
        return fullname(type(obj))
    return f'{obj.__module__}.{obj.__qualname__}'


def now():
    return datetime.now(timezone.utc).isoformat()


def sort_dict(x):
    return {k: x[k] for k in sorted(x.keys())} if isinstance(x, dict) else x


def sort_dict_recursively(x):
    if isinstance(x, list):
        return [sort_dict_recursively(y) for y in x]
    if isinstance(x, dict):
        return {k: sort_dict_recursively(x[k]) for k in sorted(x.keys())}
    if isinstance(x, set):
        return {sort_dict_recursively(v) for v in x}
    return x
