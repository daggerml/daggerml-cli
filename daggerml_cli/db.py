import lmdb
from functools import cache


@cache
def dbenv(path):
    dbs = ['index', 'head', 'commit', 'tree', 'dag', 'node', 'fnapp', 'datum']
    env = lmdb.open(path, max_dbs=len(dbs)+1)
    return env, {k: env.open_db(f'db/{k}'.encode()) for k in dbs}
