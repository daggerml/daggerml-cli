import json
import logging
import os
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field
from hashlib import md5

import lmdb

from daggerml_cli.pack import packb, unpackb
from daggerml_cli.util import makedirs

logger = logging.getLogger(__name__)


def dbenv(path, db_types, **kw):
    i = 0
    while True:
        try:
            env = lmdb.open(path, max_dbs=len(db_types) + 1, **kw)
            break
        except Exception:
            logger.exception("error while opening lmdb...")
            if i > 2:
                raise
            i += 1
    return env, {k: env.open_db(f"db/{k}".encode()) for k in db_types}


@dataclass
class Database:
    path: str
    create: InitVar[bool] = False
    repo_types: InitVar[list] = None

    def __post_init__(self, create, repo_types):
        self._tx = []
        dbfile = str(os.path.join(self.path, "data.mdb"))
        dbfile_exists = os.path.exists(dbfile)
        if create:
            assert not dbfile_exists, f"repo exists: {dbfile}"
            map_size = 10485760
            with open(os.path.join(self.path, "config"), "w") as f:
                json.dump({"map_size": map_size}, f)
        else:
            assert dbfile_exists, f"repo not found: {dbfile}"
            with open(os.path.join(self.path, "config")) as f:
                map_size = json.load(f)["map_size"]
        self.env, self.dbs = dbenv(self.path, repo_types, map_size=map_size)

    def close(self):
        self.env.close()

    def __enter__(self):
        return self

    def __exit__(self, *errs, **err_kw):
        self.close()

    def db(self, type):
        return self.dbs[type] if type else None

    @contextmanager
    def tx(self, write=False):
        cls = type(self)
        old_curr = getattr(cls, "curr", None)
        try:
            if not len(self._tx):
                self._tx.append(self.env.begin(write=write, buffers=True).__enter__())
                cls.curr = self
            else:
                self._tx.append(None)
            yield True
        finally:
            cls.curr = old_curr
            tx = self._tx.pop()
            if tx:
                tx.__exit__(None, None, None)

    def copy(self, path):
        self.env.copy(makedirs(path))

    def hash(self, obj):
        _hash = md5(packb(obj, True)).hexdigest()
        db = type(obj).__name__.lower()
        return f"{db}/{_hash}"

    def get(self, key):
        db = key.split("/", 1)[0]
        if key:
            return unpackb(self._tx[0].get(key.encode(), db=self.db(db)))

    def put(self, key, obj=None, *, return_existing=False) -> str:
        key, obj = (key, obj) if obj else (obj, key)
        assert obj is not None
        db = type(obj).__name__.lower()
        data = packb(obj)
        key2 = key or self.hash(obj)
        comp = None
        if key is None:
            comp = self._tx[0].get(key2.encode(), db=self.db(db))
            if comp not in [None, data]:
                if return_existing:
                    return key2
                msg = f"attempt to update immutable object: {key2}"
                raise AssertionError(msg)
        if key is None or comp is None:
            self._tx[0].put(key2.encode(), data, db=self.db(db))
        return key2

    def delete(self, key):
        db = key.split("/", 1)[0]
        self._tx[0].delete(key.encode(), db=self.db(db))

    def cursor(self, db):
        return map(lambda x: bytes(x[0]).decode(), iter(self._tx[0].cursor(db=self.db(db))))

    def objects(self, type=None):
        result = set()
        for db in [type] if type else list(self.dbs.keys()):
            [result.add(x) for x in self.cursor(db)]
        return result
