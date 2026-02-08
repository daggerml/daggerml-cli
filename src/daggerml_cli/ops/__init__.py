"""Main DaggerML repository class and operations.

Public API:
    Dml - Main repository class providing complete DML functionality
"""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, ContextManager, Optional

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from daggerml_cli._db import DmlDbEnv, Ref
from daggerml_cli.types import DEFAULT_HEAD, NAMESPACES

if TYPE_CHECKING:
    from daggerml_cli.ops.cache import CacheOps
    from daggerml_cli.ops.commit import CommitOps
    from daggerml_cli.ops.dag import DagOps
    from daggerml_cli.ops.gc import GcOps
    from daggerml_cli.ops.head import HeadOps
    from daggerml_cli.ops.index import IndexOps
    from daggerml_cli.ops.node import NodeOps
    from daggerml_cli.ops.remote import RemoteOps


@dataclass
class DmlOps:
    """DaggerML repository interface for managing versioned data and DAGs.
    This class provides a high-level interface for interacting with a DaggerML
    repository. It manages the database connection and exposes dynamic operation
    classes for commits, heads, indexes, DAGs, nodes, caching, and garbage collection.
    Attributes
    ----------
    path : str
        Filesystem path to the DaggerML repository.
    """

    path: str
    _db: Optional[DmlDbEnv] = None

    def __enter__(self) -> Self:
        """Enter context manager - open database connection."""
        if self._db is None:
            self._db = DmlDbEnv.open(self.path, namespaces=sorted(NAMESPACES), map_size=1024**3)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - close database connection."""
        self.close()

    def close(self):
        """Close database connection and clean up resources."""
        if self._db is not None:
            db = self._db
            self._db = None
            db.close()

    def commit(self) -> "CommitOps":
        """Return commit operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml_cli.ops.commit import CommitOps

        return CommitOps(_db=self._db)

    def head(self) -> "HeadOps":
        """Return head operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml_cli.ops.head import HeadOps

        return HeadOps(_db=self._db)

    def index(self) -> "IndexOps":
        """Return index operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml_cli.ops.index import IndexOps

        return IndexOps(_db=self._db)

    def dag(self) -> "DagOps":
        """Return DAG operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml_cli.ops.dag import DagOps

        return DagOps(_db=self._db)

    def node(self) -> "NodeOps":
        """Return node operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml_cli.ops.node import NodeOps

        return NodeOps(_db=self._db)

    def cache(self) -> "CacheOps":
        """Return cache operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml_cli.ops.cache import CacheOps

        return CacheOps(_db=self._db)

    def gc(self) -> "GcOps":
        """Return garbage collection operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml_cli.ops.gc import GcOps

        return GcOps(_db=self._db)

    def remote(self, bucket: str, prefix: str, client: Optional[Any] = None) -> "RemoteOps":
        """Return remote operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml_cli.ops.remote import RemoteOps

        if client is None:
            return RemoteOps(_db=self._db, bucket=bucket, prefix=prefix)
        else:
            return RemoteOps(_db=self._db, bucket=bucket, prefix=prefix, client=client)

    def status(self, index_ref: Optional[Ref] = None) -> dict:
        """Get repository or index status information."""
        # Implementation details...
        raise NotImplementedError("Status method is not implemented yet.")

    @classmethod
    def create(cls, path: str, user: Optional[str] = None) -> Self:
        """Create new repository at path (instantiates db instance)."""
        Path(path).mkdir(parents=True, exist_ok=False)
        db = DmlDbEnv.create(path, namespaces=sorted(NAMESPACES), map_size=1024**3)
        self = cls(_db=db, path=path)
        self.head().create(DEFAULT_HEAD.to)
        return self

    @classmethod
    def open(cls, path: str, map_size: int = 1024**3) -> Self:
        """Open existing repository (instantiates db instance).

        Parameters
        ----------
        path : str
            Directory path of the repository.
        head : Optional[Ref]
            Head reference to use, or None for DEFAULT_HEAD.
        map_size : int
            Optional LMDB map size in bytes.

        Returns
        -------
        Self
            Opened repository instance.

        Notes
        -----
        When head is None, uses DEFAULT_HEAD as the head reference
        since get_head() is not implemented in DmlDbEnv.
        """
        db = DmlDbEnv.open(path, namespaces=sorted(NAMESPACES), map_size=map_size)
        return cls(_db=db, path=path)

    @classmethod
    def temporary(cls, user: Optional[str] = None) -> ContextManager[Self]:
        """Create temporary repository for testing."""

        @contextmanager
        def _temporary():
            with TemporaryDirectory() as tmpdir:
                with cls.create(f"{tmpdir}/db", user) as repo:
                    yield repo

        return _temporary()
