"""Cache operations for managing computation results.

Public API:
    CacheOps - Class for cache management operations
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Optional

from daggerml_cli._db import DmlDbKeyNotFoundError, Ref
from daggerml_cli.ops.base_ops import BaseOps
from daggerml_cli.types import Cache, DmlRepoError


@dataclass
class CacheOps(BaseOps):
    """CRUD operations for managing cached computation results.

    Cache entries are stored at `cache:{argv_ref.id()}` and contain a `Cache`
    object mapping `argv_ref` to `result_ref`.
    """

    @staticmethod
    def _cache_ref(argspec_ref: Ref) -> Ref:
        if argspec_ref.ns() != "argspec":
            raise DmlRepoError(f"Expected argspec ref for cache key, got: {argspec_ref}")
        return Ref(f"cache:{argspec_ref.id()}")

    def _put(self, dag_ref: Ref, txn) -> Ref:
        """Create or overwrite a cache entry for `argspec_ref` within a transaction."""
        if dag_ref.ns() != "dag":
            raise DmlRepoError(f"Expected dag ref for cache value, got: {dag_ref}")
        argspec_ref = txn.get(dag_ref).argspec
        if argspec_ref is None:
            raise DmlRepoError(f"DAG {dag_ref} has no argspec, cannot cache")
        cache_ref = self._cache_ref(argspec_ref)
        txn.put(Cache(dag_ref), to=cache_ref)
        return cache_ref

    def put(self, dag_ref: Ref) -> Ref:
        """Create or overwrite a cache entry for `dag_ref`."""
        try:
            with self._tx(readonly=False) as txn:
                return self._put(dag_ref, txn)
        except Exception as e:
            raise DmlRepoError(f"Failed to put cache entry: {e}") from e

    def _get(self, argspec_ref: Ref, txn) -> Optional[Ref]:
        """Get cached result for `argspec_ref` within a transaction."""
        cache_ref = self._cache_ref(argspec_ref)
        if not txn.exists(cache_ref):
            return None
        entry: Cache = txn.get(cache_ref)
        return entry.dag

    def get(self, argspec_ref: Ref) -> Optional[Ref]:
        """Get cached result for `argspec_ref`."""
        try:
            with self._tx(readonly=True) as txn:
                return self._get(argspec_ref, txn)
        except Exception as e:
            raise DmlRepoError(f"Failed to get cache entry: {e}") from e

    def delete(self, argspec_ref: Ref) -> bool:
        """Delete cache entry for `argspec_ref`, returning whether it existed."""
        try:
            cache_ref = self._cache_ref(argspec_ref)
            with self._tx(readonly=False) as txn:
                if not txn.exists(cache_ref):
                    return False
                txn.delete(cache_ref)
                return True
        except Exception as e:
            raise DmlRepoError(f"Failed to delete cache entry: {e}") from e

    def list(self, limit: Optional[int] = None) -> Iterator[tuple[Ref, Ref]]:
        """List cache entries as (argv_ref, result_ref) pairs."""
        try:
            count = 0
            with self._tx(readonly=True) as txn:
                try:
                    for cache_ref in txn.iter("cache"):
                        entry: Cache = txn.get(cache_ref)
                        yield cache_ref, entry
                        count += 1
                        if limit is not None and count >= limit:
                            return
                except DmlDbKeyNotFoundError:
                    return
        except Exception as e:
            raise DmlRepoError(f"Failed to list cache entries: {e}") from e

    def clear(self) -> int:
        """Delete all cache entries, returning the number removed."""
        try:
            with self._tx(readonly=False) as txn:
                try:
                    refs = [ref for ref in txn.iter("cache")]
                except DmlDbKeyNotFoundError:
                    return 0
                for ref in refs:
                    txn.delete(ref)
                return len(refs)
        except Exception as e:
            raise DmlRepoError(f"Failed to clear cache entries: {e}") from e
