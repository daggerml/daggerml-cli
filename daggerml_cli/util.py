import lmdb
import msgpack
from base64 import b64encode, b64decode
from datetime import datetime, timezone
from functools import cache


@cache
def dbenv(path):
    dbs = ['index', 'head', 'commit', 'tree', 'dag', 'node', 'fnapp', 'datum']
    env = lmdb.open(path, max_dbs=len(dbs)+1)
    return env, {k: env.open_db(f'db/{k}'.encode()) for k in dbs}


def now():
    return datetime.now(timezone.utc).isoformat()


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
