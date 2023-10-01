import lmdb
import msgpack
from base64 import b64encode, b64decode
from functools import cache


@cache
def dbenv(path):
    return lmdb.open(path)


def sort_dict(x):
    if x is not None:
        return {k: x[k] for k in sorted(x.keys())}


def packb(x):
    return msgpack.packb(x)


def unpackb(x):
    return msgpack.unpackb(x) if x is not None else None


def packb64(x):
    return b64encode(packb(x)).decode()


def unpackb64(x):
    return unpackb(b64decode(x.encode()))
