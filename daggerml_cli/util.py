import os
from datetime import datetime, timezone


def assoc(xs, k, v):
    xs = xs.copy()
    xs[k] = v
    return xs


def conj(xs, x):
    return {*xs, x} if isinstance(xs, set) else [*xs, x]


def flatten(nested: list[list]) -> list:
    return [x for xs in nested for x in xs]


def asserting(x, message=None):
    if isinstance(message, str):
        assert x, message
    elif message:
        try:
            assert x
        except AssertionError as e:
            raise message from e
    else:
        assert x
    return x


def makedirs(path):
    os.makedirs(path, mode=0o700, exist_ok=True)
    return path


def readfile(path, *paths):
    if path is not None:
        p = os.path.join(path, *paths)
        if os.path.exists(p):
            with open(p) as f:
                result = f.read().strip()
                return result or None


def writefile(contents, path, *paths):
    if path is not None:
        p = os.path.join(path, *paths)
        if contents is None:
            if os.path.exists(p):
                os.remove(p)
        else:
            os.makedirs(os.path.dirname(p), mode=0o700, exist_ok=True)
            with open(p, 'w') as f:
                f.write(contents)


def fullname(obj):
    if not isinstance(obj, type):
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
