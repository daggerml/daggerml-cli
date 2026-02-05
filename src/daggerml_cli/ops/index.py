"""Index operations for managing working state and function execution.

Public API:
    IndexOps - Class for index and execution operations
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from subprocess import run
from typing import Any, Iterator, Optional
from urllib.parse import urlparse
from uuid import uuid4

from daggerml_cli._db import Ref, Resource
from daggerml_cli.builtins import BUILTIN_FNS
from daggerml_cli.ops.base_ops import BaseOps, with_resize
from daggerml_cli.ops.cache import CacheOps
from daggerml_cli.ops.dag import DagOps
from daggerml_cli.ops.node import NodeOps
from daggerml_cli.types import (
    Argspec,
    ArgvNode,
    Commit,
    Dag,
    Datum,
    DmlRepoError,
    Error,
    FnNode,
    ImportNode,
    Index,
    KwargvNode,
    LiteralNode,
    Node,
    Tree,
    require_ref,
)
from daggerml_cli.util import now, unnest


def _random_ref(ns: str) -> Ref:
    return Ref(f"{ns}:{uuid4().hex}")


@dataclass
class IndexOps(BaseOps):
    def list(self) -> Iterator[Ref]:
        """Get all index objects in db.

        Yields
        ------
        Ref
            Index object references.
        """
        with self._tx(readonly=True) as txn:
            for index_ref in txn.iter("index"):
                yield index_ref

    def start_fn(
        self,
        index_ref: Ref,
        argv: list[Ref],
        kwargv: Optional[dict[str, Ref]] = None,
        name: Optional[str] = None,
        cache: bool = True,
    ) -> Optional[Ref]:
        with self._tx(readonly=False) as txn:
            return self._start_fn(txn, index_ref, argv, kwargv, name, cache)

    def delete(self, index_ref: Ref) -> None:
        """Delete an index object from db."""
        with self._tx(readonly=False) as txn:
            txn.delete(self._validate_index_ref(index_ref))

    @with_resize
    def create(self, head: Optional[Ref] = None, dump: Optional[str] = None) -> Ref:
        """Create a new index object.

        Parameters
        ----------
        head : Ref, optional
            Reference to the head to base the index on. If None, uses the active head.
        dump : str, optional
            Optional dump payload to initialize the index from.

        Returns
        -------
        Ref
            Reference to the newly created index object.
        """
        if head is not None and head.ns() != "head":
            raise DmlRepoError(f"Expected head ref, got: {head}")
        if head is None and dump is None:
            raise DmlRepoError("Either head or dump must be provided.")
        if head is not None and dump is not None:
            raise DmlRepoError("Cannot provide both head and dump.")
        kw = {}
        with self._tx(readonly=False) as txn:
            if dump is not None:
                kw["argspec"] = txn.load(dump)
            index = self._create(head=head, **kw, txn=txn)
        return index

    @with_resize
    def get_kwargv(self, index_ref: Ref) -> Ref:
        """Return the argv node for an index (raises if missing)."""
        with self._tx(readonly=True) as txn:
            ctx = txn.get_ctx(self._validate_index_ref(index_ref))
        return DagOps(_db=self._db).get_kwargv(ctx.commit.dag)

    @with_resize
    def get_argv(self, index_ref: Ref) -> Ref:
        """Return the argv node for an index (raises if missing)."""
        with self._tx(readonly=True) as txn:
            ctx = txn.get_ctx(self._validate_index_ref(index_ref))
        return DagOps(_db=self._db).get_argv(ctx.commit.dag)

    @with_resize
    def put_import(self, index_ref: Ref, dag: Ref, node: Optional[Ref] = None, name: Optional[str] = None) -> Ref:
        """Import a node from another DAG into the current index DAG."""
        self._validate_index_ref(index_ref)
        with self._tx(readonly=False) as txn:
            ctx = txn.get_ctx(index_ref)
            dag_obj: Dag = txn.get(dag)
            # determine node (prefer explicit `node` arg)
            imported_node = node if node is not None else dag_obj.result
            if imported_node is None:
                raise DmlRepoError("Cannot import from a DAG with no result node")
            if dag == ctx.commit.dag:
                # importing from current dag is not allowed
                raise DmlRepoError("Cannot import from the current DAG")
            node_obj = ImportNode(dag, imported_node)
            return self._put_node(node_obj, name=name, txn=txn, index_ref=index_ref)

    @with_resize
    def put_literal(self, index_ref: Ref, value: Any, name: Optional[str] = None) -> Ref:
        with self._tx(readonly=False) as txn:
            return self._put_literal(value, name=name, txn=txn, index_ref=index_ref)

    @with_resize
    def commit(
        self,
        *args,
        head: Optional[Ref] = None,
        message: Optional[str] = None,
        dag_name: Optional[str] = None,
    ) -> Ref:
        """Commit the current index state with the given value as the result node.

        Supports two calling conventions for backward compatibility:
          - commit(index_ref, value, *, head=None, message=None, dag_name=None)
          - commit(value, *, head=None, message=None, dag_name=None)  # deprecated

        Returns
        -------
        Ref
            Reference to the newly created commit.

        Raises
        ------
        DmlRepoError
            If the commit operation fails.
        """
        if len(args) >= 2 and isinstance(args[0], Ref):
            index_ref = args[0]
            value = args[1]
        elif len(args) == 1:
            # Deprecated: single-arg form (value) is unsupported without an active index
            raise DmlRepoError("No active index reference. Pass index_ref explicitly to commit.")
        else:
            raise TypeError("commit() missing required arguments")

        self._validate_index_ref(index_ref)
        with self._tx(readonly=False) as txn:
            ctx = txn.get_ctx(index_ref)
            if ctx.dag is None:
                raise DmlRepoError("Index commit has no DAG.")
            if isinstance(value, Error):
                ctx.dag.error = txn.put(value)
            else:
                if value not in ctx.dag.nodes:
                    raise DmlRepoError("Value node is not part of DAG.")
                ctx.dag.result = value
            ctx.commit.dag = txn.put(ctx.dag)
            if dag_name is not None:
                ctx.tree.dags[dag_name] = ctx.commit.dag
                ctx.commit.tree = txn.put(ctx.tree)
            if message is not None:
                ctx.commit.message = message
            ctx.commit.modified = now()
            commit_ref = txn.put(ctx.commit)
            if head is not None:
                head_ctx = txn.get_ctx(head)
                head_ctx.head.commit = commit_ref
                txn.put(head_ctx.head, to=head)
            txn.delete(index_ref)
        return commit_ref

    def dump(self, commit: Ref) -> str:
        """Dump a completed Commit

        Parameters
        ----------
        commit : Ref
            Reference to the commit to dump from.

        Returns
        -------
        str
            Base64-encoded dump payload representing the commit.

        Raises
        ------
        DmlRepoError
            If the dump operation fails.
        """
        try:
            with self._tx(readonly=True) as txn:
                return txn.dump(commit)
        except Exception as e:
            raise DmlRepoError(f"Failed to dump commit: {e}") from e

    def _validate_index_ref(self, index_ref: Ref) -> Ref:
        if index_ref is None:
            raise DmlRepoError("No active index reference.")
        if index_ref.ns() != "index":
            raise DmlRepoError(f"Expected index ref, got: {index_ref}")
        return index_ref

    def _resolve_node_value_ref(self, node_ref: Ref, txn) -> Ref:
        # Validate node ref using NodeOps then return its underlying datum ref
        node_ref = NodeOps(_db=self._db)._require_node_ref(node_ref)
        node = txn.get(node_ref)
        return node.datum_ref(txn)

    def _put_node(self, node: Node, txn, index_ref: Ref, name: Optional[str] = None) -> Ref:
        self._validate_index_ref(index_ref)
        ctx = txn.get_ctx(index_ref)
        if ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")
        node_ref = txn.put(node)
        ctx.dag.nodes = sorted({node_ref, *ctx.dag.nodes})
        if name is not None:
            ctx.dag.names[name] = node_ref
        ctx.commit.dag = txn.put(ctx.dag)
        ctx.commit.modified = now()
        ctx.head.commit = txn.put(ctx.commit)
        txn.put(ctx.head, to=index_ref)
        return node_ref

    def _create(
        self,
        *,
        head: Optional[Ref] = None,
        author: Optional[str] = None,
        argspec: Optional[Ref] = None,  # -> Argspec
        txn,
    ) -> Ref:
        nodes: list[Ref] = []
        kw: dict[str, Any] = {"author": author or "DaggerML User"}
        if head is not None:
            if head.ns() != "head":
                raise DmlRepoError(f"Expected head ref, got: {head}")
            if argspec is not None:
                raise DmlRepoError("Cannot provide both head and argv.")
            base_ctx = txn.get_ctx(head)
            kw.update({"parents": [base_ctx.head.commit], "tree": base_ctx.commit.tree})
        elif argspec is not None:
            argspec_obj: Argspec = txn.get(argspec)
            nodes.append(argspec_obj.argv)
            nodes.append(argspec_obj.kwargv)
            kw.update({"parents": [], "tree": txn.put(Tree(dags={}))})
        else:
            raise DmlRepoError("Either head or argv must be provided.")
        dag_ref = txn.put(Dag(nodes=nodes, names={}, result=None, argspec=argspec))
        commit_ref = txn.put(Commit(message="", dag=dag_ref, **kw))
        idx = txn.put(Index(commit=commit_ref), to=_random_ref("index"))
        return idx

    # ~~~~~~~~~~~ START_FN ~~~~~~~~~~~
    def _prepare_fn(
        self,
        index_ref: Ref,
        argv: list[Ref],
        kwargv: dict[str, Ref],
        txn,
    ) -> tuple[Any, Ref]:
        [require_ref(arg, ["node"], "start_fn argv elements") for arg in argv]
        ctx = txn.get_ctx(index_ref)
        if ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")
        if not set(argv).issubset(set(ctx.dag.nodes)):
            raise DmlRepoError("All argv nodes must be part of current DAG.")
        fn_datum_ref = self._resolve_node_value_ref(argv[0], txn)
        if fn_datum_ref.ns() != "datum":
            raise DmlRepoError("First arg must resolve to a Datum.")
        fn_datum: Datum = txn.get(fn_datum_ref)
        fn_data = fn_datum.data
        if not isinstance(fn_data, Resource):
            raise DmlRepoError("First arg must be Resource")
        new_kwargv = {}
        for _key, value in kwargv.items():
            require_ref(value, ["node"], "start_fn kwargv values")
            if value not in ctx.dag.nodes:
                raise DmlRepoError("kwargv nodes must be part of the current DAG.")
            new_kwargv[_key] = self._resolve_node_value_ref(value, txn)
        argv_ref = txn.put(Datum([self._resolve_node_value_ref(arg, txn) for arg in argv]))
        argv_node_ref = txn.put(ArgvNode(value=argv_ref))
        kwargv_node_ref = txn.put(KwargvNode(value=txn.put(Datum(data=new_kwargv))))
        argspec_ref = txn.put(Argspec(argv=argv_node_ref, kwargv=kwargv_node_ref))
        return fn_data, argspec_ref

    def _run_builtin(self, fn: Any, argspec_ref: Ref, txn) -> Optional[Ref]:
        if fn.adapter is not None:
            return None
        fn_parsed = urlparse(fn.uri)
        if fn_parsed.scheme != "daggerml":
            raise DmlRepoError(f"Invalid builtin URI scheme: {fn_parsed.scheme}")
        fpath = fn_parsed.path.lstrip("/")
        if fpath not in BUILTIN_FNS:
            raise DmlRepoError(f"Unknown builtin: {fn_parsed} -- path: {fpath}")
        argspec: Argspec = txn.get(argspec_ref)
        kwargv_node: KwargvNode = txn.get(argspec.kwargv)
        kwargv_datum: Datum = txn.get(kwargv_node.value)
        if kwargv_datum.data != {}:
            raise DmlRepoError("Keyword arguments are not supported for builtin functions.")
        argv_node: ArgvNode = txn.get(argspec.argv)
        argv_datum: Datum = txn.get(argv_node.datum_ref(txn))
        node_ops = NodeOps(_db=self._db)
        args = [node_ops._unroll_datum_ref(arg, txn) for arg in argv_datum.data[1:]]
        result = BUILTIN_FNS[fpath](*args)
        # Create a new index for the function DAG within the current txn
        fn_index_ref = self._create(argspec=argspec_ref, txn=txn)
        # Insert the result node into the newly created index using the same txn
        result_node_ref = self._put_literal(result, name=None, txn=txn, index_ref=fn_index_ref)
        # Finalize the commit for the function index within the same txn (avoid opening new txns)
        idx_ctx = txn.get_ctx(fn_index_ref)
        if idx_ctx.dag is None:
            raise DmlRepoError("Function index has no DAG.")
        idx_ctx.dag.result = result_node_ref
        idx_ctx.commit.dag = txn.put(idx_ctx.dag)
        idx_ctx.commit.modified = now()
        commit_ref = txn.put(idx_ctx.commit)
        commit_obj: Commit = txn.get(commit_ref)
        if commit_obj.dag is None:
            raise DmlRepoError("Function commit has no DAG.")
        # clean up the temporary index object to avoid unbounded DB growth
        txn.delete(fn_index_ref)
        return commit_obj.dag

    def _call_adapter(self, fn: Any, argspec_ref: Ref, txn) -> Optional[Ref]:
        data = txn.dump(argspec_ref)
        adapter = shutil.which(getattr(fn, "adapter", None) or "")
        if not adapter:
            raise DmlRepoError(f"No such adapter: {getattr(fn, 'adapter', None)}")
        env = os.environ.copy()
        env["DML_CACHE_KEY"] = argspec_ref.id()
        result_data = run([adapter, fn.uri], input=data, capture_output=True, text=True, env=env)
        if result_data.returncode != 0:
            raise DmlRepoError(f"Adapter call failed: {result_data.stderr}")
        stdout = json.loads(result_data.stdout)
        dump = stdout.get("dump", "")
        if not dump:
            return None
        # load the returned dump into the DB using the provided txn
        commit_ref = txn.load(dump)
        commit_obj: Commit = txn.get(commit_ref)
        if commit_obj.dag is None:
            raise DmlRepoError("Function commit has no DAG.")
        return commit_obj.dag

    def _start_fn(
        self,
        txn,
        index_ref: Ref,
        argv: list[Ref],
        kwargv: Optional[dict[str, Ref]] = None,
        name: Optional[str] = None,
        cache: bool = True,
    ) -> Optional[Ref]:
        self._validate_index_ref(index_ref)
        if not isinstance(argv, list):
            raise DmlRepoError("argv must be a list of node references.")
        kwargv = kwargv or {}
        fn, argspec_ref = self._prepare_fn(index_ref, argv, kwargv, txn)
        dag_ref = self._run_builtin(fn, argspec_ref, txn)
        if dag_ref is None:
            cops = CacheOps(_db=self._db)
            if cache:
                dag_ref = cops._get(argspec_ref, txn)  # cache by argspec
            if dag_ref is None:
                # adapters are scripts that run independently and communicate via dumps
                # they can be used to implement caching internally if desired
                # They do not work on the same database.
                dag_ref = self._call_adapter(fn, argspec_ref, txn)
                if dag_ref is not None and cache:
                    cops._put(dag_ref, txn)
        if dag_ref is None:
            return None
        dag_obj: Dag = txn.get(dag_ref)
        if dag_obj.result is None and dag_obj.error is None:
            raise DmlRepoError("Function DAG has no result node.")
        out = self._put_node(
            FnNode(argv, dag_ref),
            name=name,
            txn=txn,
            index_ref=index_ref,
        )
        if dag_obj.error is not None:
            err = txn.get(dag_obj.error)
            raise err
        return out

    def _put_literal(self, value: Any, txn, index_ref: Ref, name: Optional[str] = None) -> Ref:
        self._validate_index_ref(index_ref)
        ctx = txn.get_ctx(index_ref)
        if ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")

        def _put(x) -> Ref:
            if isinstance(x, Ref):
                if not txn.exists(x):
                    raise DmlRepoError(f"Referenced object does not exist: {x}")
                if x.nss()[0] == "node":
                    if x not in ctx.dag.nodes:
                        raise DmlRepoError(f"Referenced node is not part of DAG: {x}")
                    return x
                if x.ns() == "datum":
                    return x
                raise DmlRepoError(f"Invalid reference namespace for literal value: {x.ns()}")
            if isinstance(x, tuple):
                x = list(x)
            if isinstance(x, list):
                ys = [_put(v) for v in x]
                if any(isinstance(v, Ref) and v.nss()[0] == "node" for v in ys):
                    ys = [self._put_literal(v, txn, index_ref) if v.nss()[0] != "node" else v for v in ys]
                    fn = self._put_literal(Resource(uri="daggerml:list"), txn, index_ref)
                    return self._start_fn(txn, index_ref, [fn, *ys], {})
                return txn.put(Datum(ys))
            if isinstance(x, dict):
                ys = {k: _put(v) for k, v in x.items()}
                if any(isinstance(v, Ref) and v.nss()[0] == "node" for v in ys.values()):
                    yks = [self._put_literal(k, txn, index_ref) for k in ys.keys()]
                    yvs = [self._put_literal(v, txn, index_ref) if v.nss()[0] != "node" else v for v in ys.values()]
                    fn = self._put_literal(Resource(uri="daggerml:dict"), txn, index_ref)
                    return self._start_fn(txn, index_ref, [fn, *unnest(zip(yks, yvs, strict=True))], {})
                return txn.put(Datum(ys))
            return txn.put(Datum(x))

        result_ref = _put(value)
        if result_ref.nss()[0] == "node":
            return result_ref
        # Create literal node directly in transaction
        node_ref = txn.put(LiteralNode(value=result_ref))
        ctx.dag.nodes = sorted({node_ref, *ctx.dag.nodes})
        if name is not None:
            ctx.dag.names[name] = node_ref
        ctx.commit.dag = txn.put(ctx.dag)
        ctx.commit.modified = now()
        ctx.head.commit = txn.put(ctx.commit)
        txn.put(ctx.head, to=index_ref)
        return node_ref
