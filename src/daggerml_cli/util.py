import logging
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def assoc(xs, k, v):
    xs = xs.copy()
    xs[k] = v
    return xs


def conj(xs, x):
    return {*xs, x} if isinstance(xs, set) else [*xs, x]


def flatten(nested: list[list]) -> list:
    return [x for xs in nested for x in xs]


def some(xs, default=None):
    return next((x for x in xs if x), default)


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
            with open(p, "w") as f:
                f.write(contents)


def fullname(obj):
    if not isinstance(obj, type):
        return fullname(type(obj))
    return f"{obj.__module__}.{obj.__qualname__}"


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


def as_list(x):
    return x if isinstance(x, (list, tuple)) else [x]


def merge_counters(x, *xs):
    if not len(xs):
        return x
    y, rest = xs[0], xs[1:]
    result = {}
    for k in set(x.keys()).union(set(y.keys())):
        result[k] = flatten([as_list(x.get(k, 0)), as_list(y.get(k, 0))])
    return merge_counters(result, *rest) if len(rest) else result


def detect_executable(name, regex):
    try:
        path = shutil.which(name)
        out = subprocess.run(
            [path, "--version"],
            text=True,
            capture_output=True,
        ).stdout.split("\n", 1)[0]
        return path if re.search(regex, out) else None
    except Exception:
        pass


def run(args, input=None, text=True):
    proc = subprocess.run(
        args,
        input=input,
        capture_output=True,
        text=text,
        check=False,
    )
    err = "" if not proc.stderr else f"\n{proc.stderr.rstrip()}"
    if proc.stderr:
        log.error(proc.stderr.rstrip())
    assert proc.returncode == 0, f"{args[0]}: exit status: {proc.returncode}{err}"
    return proc.stdout
