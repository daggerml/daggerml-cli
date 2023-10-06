from datetime import datetime, timezone


def fullname(obj):
    if type(obj) != type:
        return fullname(type(obj))
    return f'{obj.__module__}.{obj.__qualname__}'


def now():
    return datetime.now(timezone.utc).isoformat()


def sort_dict(x):
    if x is not None:
        return {k: x[k] for k in sorted(x.keys())}
