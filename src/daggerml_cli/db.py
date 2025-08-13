import json
import logging
import math
import os
import shutil
import subprocess
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field
from typing import Optional, cast

import lmdb

from daggerml_cli.util import makedirs

logger = logging.getLogger(__name__)
MAP_SIZE_MIN = 128 * 1024**2  # Minimum 128MB
MAP_SIZE_MAX = 128 * 1024**3  # Maximum 128GB


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


def get_map_size(path=None, env=None):
    if env is not None:
        curr_size = env.info()["map_size"]
        logger.debug("Current LMDB map size from env: %r", curr_size)
    else:
        curr_size = [f"{path}/{f}" for f in os.listdir(path)]
        curr_size = [f for f in curr_size if os.path.isfile(f)]
        curr_size = sum(os.stat(f).st_size for f in curr_size)
        logger.debug("Current LMDB map size from path %r: %r", path, curr_size)
    if curr_size >= MAP_SIZE_MAX:
        msg = f"LMDB map size is already at maximum: {curr_size}"
        raise RuntimeError(msg)
    map_size = min(int(math.ceil(curr_size + MAP_SIZE_MIN)), MAP_SIZE_MAX)
    logger.info("Setting LMDB map_size to %r", map_size)
    return map_size


@dataclass
class Cache:
    path: str
    env: Optional[lmdb.Environment] = field(init=False, default=None)
    create: InitVar[bool] = False

    def __post_init__(self, create=False):
        if create:
            assert not os.path.exists(self.path), f"cache exists: {self.path}"
            makedirs(self.path)
        for _ in range(3):
            try:
                self.env = lmdb.open(self.path, max_dbs=1, map_size=get_map_size(self.path))
                break
            except lmdb.Error as e:
                logger.exception("LMDB error while opening environment: %s", e)
                if _ == 2:
                    raise

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
                self.env.set_mapsize(get_map_size(env=self.env))

    def get(self, key: str) -> Optional[str]:
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
            return tx.delete(key.encode())

        return self._resize_call(inner, write=True)

    def __iter__(self):
        def inner(tx):
            with tx.cursor() as cursor:
                return sorted(
                    [cast(dict, self.describe(key.decode(), cast(bytes, val).decode())) for key, val in cursor],
                    key=lambda x: x["cache_key"],
                )

        return iter(self._resize_call(inner))

    def describe(self, key, val=None):
        val = val or self.get(key)
        if val is None:
            return None
        js = cast(list, json.loads(val))
        if js[0] == "Error":
            return {"cache_key": key, "error": True, "data": None, "dag_id": None}
        return {"cache_key": key, "error": False, "data": js, "dag_id": js[-1][1][1]}

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
                return cast(bytes, cached_val).decode()
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
