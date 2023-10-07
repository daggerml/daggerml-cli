from datetime import datetime, timezone


def walk_type(*fields):
    def decorator(cls):
        cls.walk = fields if isinstance(fields[0], str) else fields[0]
        return cls
    return decorator


def walk_fields(obj):
    if hasattr(obj.__class__, 'walk'):
        if isinstance(obj.__class__.walk, type(lambda: None)):
            return obj.__class__.walk(obj)
        return obj.__class__.walk
    return []


def uuid_type(cls):
    cls.uuid = True
    return cls


def is_uuid(obj):
    return hasattr(obj.__class__, 'uuid')


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
