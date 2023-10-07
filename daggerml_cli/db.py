import lmdb
from functools import cache


DB_TYPES = []


@cache
def dbenv(path):
    global DB_TYPES
    env = lmdb.open(path, max_dbs=len(DB_TYPES)+1)
    return env, {k: env.open_db(f'db/{k}'.encode()) for k in DB_TYPES}


def db_type(cls):
    global DB_TYPES
    DB_TYPES.append(cls.__name__.lower())
    return cls
