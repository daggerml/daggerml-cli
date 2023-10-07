from datetime import datetime, timezone


def fullname(obj):
    if type(obj) != type:
        return fullname(type(obj))
    return f'{obj.__module__}.{obj.__qualname__}'


def now():
    return datetime.now(timezone.utc).isoformat()


def conj(x, y):
    x.append(y)
    return x


def sort_dict(x):
    return {k: x[k] for k in sorted(x.keys())} if isinstance(x, dict) else x


def sort_dict_recursively(x):
    if isinstance(x, list):
        return [sort_dict_recursively(y) for y in x]
    elif isinstance(x, dict):
        return {k: sort_dict(v) for k, v in x.items()}
    elif isinstance(x, set):
        return {sort_dict_recursively(v) for v in x}
    else:
        return x
