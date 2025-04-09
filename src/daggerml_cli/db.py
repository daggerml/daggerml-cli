import logging

import lmdb

logger = logging.getLogger(__name__)
DB_TYPES = []


def dbenv(path, **kw):
    i = 0
    while True:
        try:
            env = lmdb.open(path, max_dbs=len(DB_TYPES) + 1, **kw)
            break
        except Exception:
            logger.exception("error while opening lmdb...")
            if i > 2:
                raise
            i += 1
    return env, {k: env.open_db(f"db/{k}".encode()) for k in DB_TYPES}


def db_type(cls):
    DB_TYPES.append(cls.__name__.lower())
    return cls
