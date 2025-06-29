import json
import logging
import math
import os
import shutil
import subprocess
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field
from typing import Optional

import lmdb

from daggerml_cli.util import makedirs

logger = logging.getLogger(__name__)
MAP_SIZE_HEADROOM = 2  # x more than current size
MAP_SIZE_MIN = 128 * 1024**2  # Minimum 128MB
MAP_SIZE_MAX = 128 * 1024**3  # Hard cap 64GB


class CacheError(Exception):
    """Custom exception for cache-related errors."""


def serialize_resource(x):
    from daggerml_cli.repo import Resource

    if isinstance(x, Resource):
        return {
            "__type__": "resource",
            "uri": x.uri,
            "data": x.data,
            "adapter": x.adapter,
        }


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
class Cache:
    path: str
    env: Optional[lmdb.Environment] = field(init=False, default=None)
    create: InitVar[bool] = False
    map_size: InitVar[Optional[int]] = None

    def __post_init__(self, create=None, map_size=None):
        if create:
            assert not os.path.exists(self.path), f"cache exists: {self.path}"
            makedirs(self.path)
        if map_size is None:
            map_size = [f"{self.path}/{f}" for f in os.listdir(self.path)]
            map_size = [f for f in map_size if os.path.isfile(f)]
            map_size = sum(os.stat(f).st_size for f in map_size)
            if map_size < MAP_SIZE_MIN:
                logger.info("Db too small... setting size %r -> %r", map_size, MAP_SIZE_MIN)
                map_size = MAP_SIZE_MIN
        map_size = map_size / MAP_SIZE_HEADROOM
        for _ in range(3):
            try:
                self.env = lmdb.open(self.path, max_dbs=1, map_size=self._get_size(map_size))
                break
            except lmdb.Error as e:
                logger.exception("LMDB error while opening environment: %s", e)
                if _ == 2:
                    raise

    def _get_size(self, curr_size=None):
        if curr_size is None:
            curr_size = self.env.info()["map_size"]
        if curr_size >= MAP_SIZE_MAX:
            msg = f"LMDB map size is already at maximum: {curr_size}"
            raise RuntimeError(msg)
        new_size = min(int(math.ceil(curr_size * MAP_SIZE_HEADROOM)), MAP_SIZE_MAX)
        logger.info("Growing LMDB map_size from %r to %r", curr_size, new_size)
        return new_size

    def resize(self, curr_size=None):
        self.env.set_mapsize(self._get_size(curr_size))

    @contextmanager
    def tx(self, write=False):
        with self.env.begin(write=write) as tx:
            yield tx

    def _resize_call(self, func, write=False):
        while True:
            try:
                with self.tx(write=write) as tx:
                    return func(tx)
            except lmdb.MapFullError:
                self.resize()

    def get(self, key):
        def inner(tx):
            data = tx.get(key.encode())
            if data is not None:
                data = data.decode()
            return data

        return self._resize_call(inner)

    def put(self, key, value, old_value=None):
        def inner(tx):
            old_val = tx.get(key.encode())
            if old_val != old_value:
                raise CacheError(f"Cache key {key!r} failed the value check")
            data = value.encode()
            tx.put(key.encode(), data)

        self._resize_call(inner, write=True)

    def delete(self, key):
        def inner(tx):
            tx.delete(key.encode())

        self._resize_call(inner, write=True)
        return True

    def __iter__(self):
        def inner(tx):
            with tx.cursor() as cursor:
                return sorted(
                    [
                        {
                            "cache_key": key.decode(),
                            "dag_id": json.loads(val.decode())[-1][1][1],
                        }
                        for key, val in cursor
                    ],
                    key=lambda x: x["cache_key"],
                )

        return iter(self._resize_call(inner))

    def _close(self):
        if self.env is not None:
            self.env.close()
            self.env = None
        else:
            logger.warning("Cache environment already closed or never opened.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close()
        if exc_type is not None:
            logger.error("Exception occurred: %s", exc_value, exc_info=True)
        return False

    def submit(self, fn, cache_key, dump):
        # all in one transaction to avoid race conditions and muitiple calls to adapter
        with self.tx(True) as tx:
            cached_val = tx.get(cache_key.encode())
            if cached_val:
                return cached_val.decode()
            cmd = shutil.which(fn.adapter or "")
            assert cmd, f"no such adapter: {fn.adapter}"
            payload = json.dumps(
                {
                    "cache_path": self.path,
                    "cache_key": cache_key,
                    "kwargs": fn.data,
                    "dump": dump,
                },
                default=serialize_resource,
            )
            env = os.environ.copy()
            env["DML_CACHE_PATH"] = self.path
            env["DML_CACHE_KEY"] = cache_key
            proc = subprocess.run([cmd, fn.uri], input=payload, capture_output=True, text=True, env=env)
            if proc.stderr:
                logger.error(proc.stderr.rstrip())
            assert proc.returncode == 0, f"{cmd}: exit status: {proc.returncode}\n{proc.stderr}"
            resp = proc.stdout
            if resp:
                try:
                    tx.put(cache_key.encode(), resp.encode())
                    return resp
                except lmdb.MapFullError:
                    self.put(cache_key, resp)
            return resp
