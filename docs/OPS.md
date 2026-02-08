# Ops (Core Repository Operations)

This document specifies the public API for the `src/daggerml_cli/ops/` module.

## Scope

This document specifies the contracts for the core repository “ops” layer implemented under `src/daggerml_cli/ops/`.

In-scope:

- Local repository operations: heads, commits, indexes (working state), DAGs, nodes, cache, GC.
- Remote repository operations: S3-backed CAS + refs (`src/daggerml_cli/ops/remote.py`).
- The adapter execution protocol used by `IndexOps.start_fn()`.
- The dump/load manifest format used by `TxnContext.dump*()` and `TxnContext.load*()`.

Out-of-scope:

- The internal LMDB wrapper implementation (`daggerml_cli._db`) except where its types/protocols are directly consumed by ops public surfaces.
- The full semantic specification of DML graph evaluation and node/type schemas beyond what ops methods require.

## Public Surface Rule (Enforced)

Any interface used outside its defining module/file is part of the public surface.

Consequences for `src/daggerml_cli/ops/`:

- `daggerml_cli.ops.base_ops.BaseOps`, `daggerml_cli.ops.base_ops.TxnContext`, and `daggerml_cli.ops.base_ops.with_retry` are public because they are imported by other modules.
- All `*Ops` classes returned by `DmlOps` subsystem constructors are public.
- Protocol surfaces include environment variables, JSON formats, and subprocess adapter I/O.

Consumers MUST treat any ambiguity in public surface contracts as a design bug: all public behavior must be explicitly specified in this document.

For each public interface (types, functions/methods, env vars, file formats, protocol messages), this document specifies:

- Location/name and signature/shape
- Preconditions (including Ref namespace constraints)
- Observable behavior (including side effects)
- Errors

Checkable invariants are expressed via:

- `Requires` clauses
- `Returns` clauses
- Explicit `Invariants` clauses where additional properties must hold

Any behavior not specified by `Requires`/`Behavior`/`Errors`/`Returns`/`Invariants` is intentionally undefined.

## Terms

- Repo: a directory containing an LMDB environment opened/created via `daggerml_cli._db.DmlDbEnv`.
- Ref: `daggerml_cli._db.Ref`, an object reference string of the form `"<namespace>:<id>"`.
- Namespace: the `Ref.ns()` string. Many ops methods validate namespaces (e.g. `"head"`, `"commit"`, `"dag"`, `"index"`, etc.).
- Object types: dataclasses in `daggerml_cli.types` stored in the DB; ops treat them as opaque except where accessed.

## Module Map

- `src/daggerml_cli/ops/__init__.py`
- `src/daggerml_cli/ops/base_ops.py`
- `src/daggerml_cli/ops/commit.py`
- `src/daggerml_cli/ops/head.py`
- `src/daggerml_cli/ops/index.py`
- `src/daggerml_cli/ops/dag.py`
- `src/daggerml_cli/ops/node.py`
- `src/daggerml_cli/ops/cache.py`
- `src/daggerml_cli/ops/gc.py`
- `src/daggerml_cli/ops/remote.py`

## Repo Entry Point: `daggerml_cli.ops.DmlOps`

Location: `src/daggerml_cli/ops/__init__.py:28`.

`DmlOps` is the canonical repo API name at this time.

### Type

`@dataclass class DmlOps:`

Fields:

- `path: str` (filesystem path to repo directory)
- `_db: Optional[daggerml_cli._db.DmlDbEnv]` (open DB environment; may be `None`)

### Lifecycle

- `DmlOps.__enter__() -> Self`
  - Behavior: opens `_db` if currently `None`.
  - Side effects: opens LMDB env at `self.path`.
  - Errors: `RuntimeError` may surface from `DmlDbEnv.open`.

- `DmlOps.__exit__(exc_type, exc_val, exc_tb) -> None`
  - Behavior: calls `close()`.

- `DmlOps.close() -> None`
  - Behavior: if `_db` is set, closes it and sets `_db=None`.

### Constructors

- `@classmethod DmlOps.create(path: str, user: Optional[str] = None) -> Self`
  - Behavior:
    - Creates directory `path` (`exist_ok=False`).
    - Creates new DB env with namespaces `sorted(daggerml_cli.types.NAMESPACES)` and `map_size=1024**3`.
    - Creates the default head: `self.head().create(daggerml_cli.types.DEFAULT_HEAD.to)`.
  - Side effects: filesystem directory creation; DB creation.
  - Errors:
    - `FileExistsError` if `path` already exists.
    - Any `daggerml_cli.types.DmlRepoError` raised by head creation.
    - Any exceptions raised by `DmlDbEnv.create`.
  - Notes: `user` is currently unused.

- `@classmethod DmlOps.open(path: str, map_size: int = 1024**3) -> Self`
  - Behavior: opens an existing repo DB env at `path` with provided `map_size`.
  - Side effects: opens LMDB env.
  - Errors: any exceptions raised by `DmlDbEnv.open`.

- `@classmethod DmlOps.temporary(user: Optional[str] = None) -> ContextManager[Self]`
  - Behavior: returns a context manager that creates a temporary directory, then `DmlOps.create(f"{tmpdir}/db", user)`.
  - Side effects: filesystem temp dir creation and cleanup.
  - Notes: `user` is currently unused.

### Subsystem Constructors

Each constructor requires `_db is not None`.

Common error:

- Raises `RuntimeError("Database is not open.")` if `_db` is `None`.

Invariants:

- Each constructor returns a new ops object bound to the same `DmlDbEnv` instance.
- Constructors do not open/close the database.
- Constructors do not start transactions.
- Constructors lazy-load their implementation modules for performance.

- `DmlOps.commit() -> daggerml_cli.ops.commit.CommitOps`
- `DmlOps.head() -> daggerml_cli.ops.head.HeadOps`
- `DmlOps.index() -> daggerml_cli.ops.index.IndexOps`
- `DmlOps.dag() -> daggerml_cli.ops.dag.DagOps`
- `DmlOps.node() -> daggerml_cli.ops.node.NodeOps`
- `DmlOps.cache() -> daggerml_cli.ops.cache.CacheOps`
- `DmlOps.gc() -> daggerml_cli.ops.gc.GcOps`
- `DmlOps.remote(bucket: str, prefix: str, client: Optional[Any] = None) -> daggerml_cli.ops.remote.RemoteOps`
  - Behavior:
    - Constructs and returns `RemoteOps(_db=self._db, bucket=bucket, prefix=prefix, client=client)`.
    - If `client is None`, `RemoteOps` uses its internal default (`boto3.client("s3")`).
  - Notes:
    - `bucket` and `prefix` are explicit configuration; `DmlOps.remote(...)` does not read environment variables.

## Base Transaction Layer

### `daggerml_cli.ops.base_ops.with_retry`

Location: `src/daggerml_cli/ops/base_ops.py:53`.

Signature: `with_retry(fn) -> callable`.

Behavior:

- Wraps `fn(self, *args, **kwargs)`.
- If `daggerml_cli._db.DmlDbMapFullError` is raised, doubles the LMDB map size and retries.
- If `daggerml_cli._db.DmlDbEnvReopenedError` is raised, retries the operation (environment was repaired).

Side effects:

- Logs a warning and calls `self._db.resize(new_map_size)`.

Notes:

- The wrapper loops until `fn` succeeds.

### `daggerml_cli.ops.base_ops.TxnContext`

Location: `src/daggerml_cli/ops/base_ops.py:98`.

`TxnContext` is the transaction-bound API used by ops implementations. It is public because it is referenced outside `base_ops.py`.

Fields:

- `db: daggerml_cli._db.DmlDbEnv`
- `txn: daggerml_cli._db.DmlDbEnvTxn`
- `logger: logging.Logger`

Methods:

- `TxnContext.put(obj: Any, to: Optional[Ref] = None) -> Ref`
  - Behavior:
    - If `obj` is a `Ref`, stores it directly (used for ref-valued pointers like heads).
    - Otherwise requires `obj._validate()` and stores `obj.to_dict()`.
    - If `to` is provided, writes at that ref; else generates a new ref in `obj._ns`.
    - For `Datum(Resource)` objects that are not `Deletable`, calls `_cleanup_deleted_entry(ref)`.
  - Errors:
    - `ValueError` for unknown namespace.
    - `daggerml_cli.types.DmlRepoError` for failures.

- `TxnContext.get(ref: Ref) -> Any`
  - Behavior: loads object dict at `ref`, maps `ref.ns()` to a class via `daggerml_cli.types.NAMESPACES`, and calls `cls.from_dict()`.
  - Errors:
    - `daggerml_cli.types.DmlRepoError("Object not found: {ref}")` when missing.
    - Re-raises `daggerml_cli.types.Error` subclasses.

- `TxnContext.delete(ref: Ref) -> None`
- `TxnContext.exists(ref: Ref) -> bool`

- `TxnContext.iter(namespace: str) -> Iterator[Ref]`
  - Behavior: yields refs from `txn.iter(namespace)`; if the underlying iterator yields `(ref, value)` tuples, yields only `ref`.

#### Dump/Load (Local Manifest)

The dump/load format is a JSON object called a “local manifest”. It is used by:

- `IndexOps.dump(commit_ref) -> str`
- `IndexOps.create(dump=...) -> Ref`
- `IndexOps._call_adapter()` (stdin/stdout protocol)
- `RemoteOps.push()` and `RemoteOps.pull()`

Format (canonical keys):

```json
{
  "schema": 0,
  "kind": "local-manifest",
  "root-ns": "<namespace>",
  "root-id": "<id>",
  "closure": {
    "<namespace>": {
      "<id>": "<raw-dump-string>",
      "...": "..."
    }
  }
}
```

- `TxnContext.dump(ref: Ref) -> str`
  - Behavior: returns `json.dumps(TxnContext.dump_dict(ref), separators=(",", ":"))`.
  - Invariants:
    - Return value is valid JSON.
    - Return value is compact (no spaces after separators).

- `TxnContext.dump_dict(ref: Ref) -> dict`
  - Behavior:
    - Traverses the object graph starting at `ref`.
    - Produces `closure` as a mapping `{ns: {id: dump}}` where `dump` is the raw DB value.
  - Invariants:
    - Top-level keys include `schema=0`, `kind="local-manifest"`, `root-ns`, `root-id`, `closure`.

- `TxnContext.load(payload: str) -> Ref`
  - Behavior: parses JSON and calls `load_dict`.
  - Invariants:
    - `payload` must be valid JSON.

- `TxnContext.load_dict(manifest: dict) -> Ref`
  - Validation:
    - `schema == 0` and `kind == "local-manifest"`.
    - Must include `root-ns`, `root-id`, and `closure[root-ns][root-id]`.
  - Behavior:
    - Inserts each closure entry using raw put.
    - Verifies inserted IDs match the manifest IDs.
    - Returns `Ref(f"{root-ns}:{root-id}")`.
  - Invariants:
    - Loading the same `manifest` twice is allowed (it may overwrite identical raw values at identical IDs).

### `daggerml_cli.ops.base_ops.BaseOps`

Location: `src/daggerml_cli/ops/base_ops.py:501`.

`BaseOps` is the base class used by all subsystem ops classes.

Fields:

- `_db: daggerml_cli._db.DmlDbEnv`

Methods:

- `BaseOps.__post_init__() -> None`
  - Behavior: initializes `self._logger`.

- `BaseOps._tx(readonly: bool = False) -> contextmanager[TxnContext]`
  - Behavior:
    - Opens a DB transaction via `self._db.tx(readonly=readonly)`.
    - Yields a `TxnContext(db=self._db, txn=txn, logger=self._logger)`.
  - Notes:
    - Logs: “Nested transactions are not supported. Readonly flag will be ignored.”
    - Does not actually detect nesting; it always delegates to `_db.tx()`.
  - Errors:
    - Re-raises `daggerml_cli.types.Error`.
    - Wraps other exceptions in `daggerml_cli.types.DmlRepoError`.

- `BaseOps._with_ops(**changes) -> Self`
  - Behavior: mutates `self` by `setattr` for each `changes` key, then returns `self`.

## Local Ops Subsystems

### `daggerml_cli.ops.commit.CommitOps`

Location: `src/daggerml_cli/ops/commit.py:20`.

Public methods:

- `CommitOps.list(head: Ref, limit: Optional[int] = None) -> Iterator[Ref]`
  - Requires: `head.ns() == "commit"` (no head indirection).
  - Behavior: yields commits by following `Commit.parents[0]`.
  - Errors: `DmlRepoError` on invalid namespace or read failures.

- `CommitOps.merge(commit1: Ref, commit2: Ref, user: str) -> Ref`
  - Behavior: creates a merge commit with `parents=[commit1, commit2]` and a merged tree.
  - Errors: `DmlRepoError` on merge conflicts or DB failures.

- `CommitOps.rebase(source: Ref, target: Ref, user: str) -> Ref`
  - Behavior:
    - If merge-base equals `source`, returns `target`.
    - If merge-base equals `target`, returns `source`.
    - Otherwise, replays a single linear commit onto `target`.
  - Errors: `DmlRepoError` if non-linear history or DB failures.

- `CommitOps.get_dag(commit: Ref, name: str) -> Optional[Ref]`
  - Behavior: returns `Tree.dags.get(name)` for the commit.
  - Errors: `DmlRepoError` on missing objects or wrong types.

- `CommitOps.delete_dag(name: str, head: Ref, user: str) -> Self`
  - Behavior:
    - Creates a new commit that removes `name` from the head commit's tree.
    - Updates the provided `head` ref to point at the new commit.
  - Returns: `self` (for method chaining).
  - Errors: `DmlRepoError` if DAG not found or DB failures.

### `daggerml_cli.ops.head.HeadOps`

Location: `src/daggerml_cli/ops/head.py:22`.

Public methods:

- `HeadOps.list() -> list[Ref]`
  - Behavior: returns all refs yielded by iterating namespace `"head"`.

- `HeadOps.create(branch_name: str, from_head: Ref | None = None) -> Head`
  - Behavior:
    - Writes a head object at `Ref(f"head:{branch_name}")`.
    - If `from_head is None`, creates an initial commit with empty `Tree(dags={})`.
    - If `from_head.ns() == "head"`, points new branch to `txn.get(from_head).commit`.
    - If `from_head.ns() == "commit"`, points new branch directly to `from_head`.
  - Errors: `DmlRepoError` if branch exists, source missing, or invalid namespace.

- `HeadOps.delete(head_ref: Ref) -> None`
  - Requires: `head_ref.ns() == "head"`.
  - Behavior: deletes `head_ref`.
  - Errors: `DmlRepoError` if head missing or invalid ref.

### `daggerml_cli.ops.dag.DagOps`

Location: `src/daggerml_cli/ops/dag.py:15`.

Public methods:

- `DagOps.list() -> list[dict[str, Any]]`
  - Behavior: returns one entry per `dag` namespace ref with keys: `name`, `id`, `nodes`, `names`, `result`, `argspec`.

- `DagOps.describe(dag_ref: Ref) -> dict`
  - Requires: `dag_ref.ns() == "dag"`.

- `DagOps.get_node(dag_ref: Ref, name: str) -> Ref`
  - Requires: `dag_ref.ns() == "dag"` and DAG must be finished (`dag.is_finished()` must be true).
  - Errors: `DmlRepoError` if unfinished DAG, missing DAG, or missing name.

- `DagOps.get_argv(dag_ref: Ref) -> Ref`
- `DagOps.get_kwargv(dag_ref: Ref) -> Ref`
  - Requires: `dag_ref.ns() == "dag"` and `dag.argspec is not None`.
  - Returns: `Argspec.argv` / `Argspec.kwargv`.

### `daggerml_cli.ops.node.NodeOps`

Location: `src/daggerml_cli/ops/node.py:21`.

Public methods:

- `NodeOps.get(node_ref: Ref) -> Any`
  - Requires: `node_ref.nss()[0] == "node"`.
  - Behavior: retrieves the node’s datum and returns `Datum.data` (collections may contain nested datum refs).
  - Errors: `DmlRepoError` if invalid ref or missing objects.

- `NodeOps.unroll(node_ref: Ref) -> Any`
  - Requires: `node_ref.nss()[0] == "node"`.
  - Behavior: fully realizes nested datum refs into pure Python values (lists/dicts/scalars).
  - Errors:
    - `DmlRepoError` for invalid refs.
    - `DmlRepoError("Cycle detected while unrolling datum: ...")` if datum references form a cycle.
    - `DmlRepoError("Cannot unroll error value.")` if encountering an `error:` datum.

### `daggerml_cli.ops.cache.CacheOps`

Location: `src/daggerml_cli/ops/cache.py:17`.

Keying invariant:

- Cache entries are stored at `Ref(f"cache:{argspec_ref.id()}")`.

Public methods:

- `CacheOps.put(dag_ref: Ref) -> Ref`
  - Requires: `dag_ref.ns() == "dag"` and `txn.get(dag_ref).argspec is not None`.
  - Behavior: writes `Cache(dag_ref)` to the cache ref derived from the dag’s argspec.

- `CacheOps.get(argspec_ref: Ref) -> Optional[Ref]`
  - Requires: `argspec_ref.ns() == "argspec"`.
  - Returns: the cached dag ref if present; else `None`.

- `CacheOps.delete(argspec_ref: Ref) -> bool`
  - Returns: `True` if an entry existed and was deleted; else `False`.

- `CacheOps.list(limit: Optional[int] = None) -> Iterator[tuple[Ref, Ref]]`
  - Yields: `(cache_ref, entry)` pairs where:
    - `cache_ref`: `Ref("cache:<argspec_id>")` identifying the cache entry key.
    - `entry`: `Cache` object containing the cached result DAG ref.
  - Ordering: unspecified (iteration order is whatever the underlying DB provides).
  - Note: To get argspec_ref from cache_ref, parse the ID portion; to get dag_ref, use `entry.dag`.

- `CacheOps.clear() -> int`
  - Returns: number of deleted cache entries.

### `daggerml_cli.ops.gc.GcOps`

Location: `src/daggerml_cli/ops/gc.py:18`.

Public methods:

- `GcOps.list_orphans(heads: list[Ref] | None = None) -> list[Ref]`
  - Behavior:
    - If `heads is None`, uses all `head` and `index` refs as traversal roots.
    - If `heads` is `[]` (empty), delegates to DB to compute orphans across the entire database.
    - Returns `list(txn.txn.list_orphans(heads))`.
  - Errors: `DmlRepoError` on failures.

- `GcOps.gc() -> dict[str, int]`
  - Behavior:
    - Computes orphans via `list_orphans()`.
    - In a write transaction, deletes each orphan if it exists.
    - Returns counts by namespace.

### `daggerml_cli.ops.index.IndexOps`

Location: `src/daggerml_cli/ops/index.py:48`.

Concept:

- An “index” is a mutable working state for constructing a DAG/commit. It is stored as a `Ref` in namespace `"index"`.

Public methods:

- `IndexOps.list() -> Iterator[Ref]`
  - Behavior: yields all `index` refs.

- `IndexOps.create(head: Optional[Ref] = None, dump: Optional[str] = None) -> Ref`
  - Requires:
    - Exactly one of `head` or `dump` must be provided.
    - If `head` is provided: `head.ns() == "head"`.
  - Behavior:
    - If `dump` is provided: `txn.load(dump)` to produce an `argspec` ref.
    - Creates a new `Dag` and `Commit`, then stores an `Index(commit=commit_ref)` at a random `index:<uuid>` ref.
  - Decorator: `@with_retry`.

- `IndexOps.delete(index_ref: Ref) -> None`
  - Requires: `index_ref.ns() == "index"`.

- `IndexOps.put_import(index_ref: Ref, dag: Ref, node: Optional[Ref] = None, name: Optional[str] = None) -> Ref`
  - Requires:
    - `index_ref.ns() == "index"`.
    - `dag.ns() == "dag"`.
    - If `node` is not provided, imports `Dag.result` from `dag`.
    - Cannot import from the index’s current DAG (`dag != ctx.commit.dag`).
  - Errors: `DmlRepoError` if imported DAG has no result node.

- `IndexOps.put_literal(index_ref: Ref, value: Any, name: Optional[str] = None) -> Ref`
  - Behavior:
    - Converts `value` into a `Datum` graph.
    - If `value` contains node refs inside lists/dicts, it may construct function nodes using the builtins `daggerml:list` and `daggerml:dict`.
    - Returns a node ref when inserting into the DAG; may return an existing node ref when `value` is already a node ref.
  - Decorator: `@with_retry`.

- `IndexOps.start_fn(index_ref: Ref, argv: list[Ref], kwargv: Optional[dict[str, Ref]] = None, name: Optional[str] = None, cache: bool = True) -> Optional[Ref]`
  - Behavior:
    - Treats `argv[0]` as the function value node.
    - Supports builtins (`Resource.uri` scheme `daggerml://...` with `adapter is None`).
    - Otherwise runs an adapter subprocess (see Adapter Protocol).
    - Optionally caches by argspec via `CacheOps`.
    - Inserts an `FnNode(argv, dag_ref)` into the current index’s DAG and returns its node ref.
    - If the called function DAG has an error, raises that error.
  - Returns: node ref for the function application, or `None` if no dag was produced.

- `IndexOps.commit(*args, head: Optional[Ref] = None, message: Optional[str] = None, dag_name: Optional[str] = None) -> Ref`
  - Calling convention:
    - Supported: `commit(index_ref: Ref, value: Ref, *, head=None, message=None, dag_name=None)`.
    - Unsupported/deprecated: `commit(value)` (raises `DmlRepoError`).
  - Behavior:
    - Sets the index DAG’s result (or error) and writes a new `Commit`.
    - If `dag_name` is provided, writes `tree.dags[dag_name] = commit.dag` and updates the commit’s tree.
    - If `head` is provided, updates that head to point to the new commit.
    - Deletes the `index_ref` (index is consumed).
  - Errors:
    - `DmlRepoError` if DAG missing, value not part of DAG, or invalid refs.

- `IndexOps.dump(commit: Ref) -> str`
  - Returns: a local-manifest JSON string for `commit` via `TxnContext.dump()`.

- `IndexOps.get_argv(index_ref: Ref) -> Ref`
- `IndexOps.get_kwargv(index_ref: Ref) -> Ref`
  - Behavior: loads the index’s commit DAG and delegates to `DagOps.get_argv`/`get_kwargv`.

#### Adapter Protocol (Subprocess)

When a function node’s `Resource` has a non-`None` `adapter` field, `IndexOps` executes the adapter program:

- Executable discovery: `shutil.which(fn.adapter)` must return a path.
- Subprocess invocation: `run([adapter_exe, fn.uri], input=<dump>, capture_output=True, text=True, env=env)`.
- Environment variables:
  - `DML_CACHE_KEY`: set to `argspec_ref.id()`.
- stdin: a local-manifest JSON string for the argspec ref.
- stdout: JSON with optional key `"dump"`.
  - If `dump` is missing/empty, adapter is treated as producing no DAG (`None`).
  - If `dump` is present, it must be a valid local-manifest JSON string; it is loaded via `TxnContext.load()` and must yield a `Commit` with a non-`None` `dag`.
- Errors:
  - Non-zero returncode raises `DmlRepoError(f"Adapter call failed: {stderr}")`.
  - Invalid JSON / invalid dump raises `DmlRepoError`.

## Remote Ops (`S3`)

### `daggerml_cli.ops.remote.RemoteOps`

Location: `src/daggerml_cli/ops/remote.py:75`.

Construction:

- `RemoteOps(client=..., bucket=..., prefix=...)`.
- Default `client` is `boto3.client("s3")`.
- If `boto3` is not installed, constructing the default client raises `ImportError("boto3 is required for RemoteOps but is not installed.")`.

On initialization, `RemoteOps.__post_init__()` calls `_ensure_remote_descriptor()`.

### Remote Layout and Descriptor

Remote layout is “cas+refs” with a required JSON descriptor stored at:

- Key: `"dml.json"` or `"{prefix}/dml.json"` if `prefix` is set.

Descriptor contents (canonical JSON, `sort_keys=True`, compact separators):

```json
{
  "schema": 0,
  "hash": "sha256",
  "layout": "cas+refs",
  "refs_prefix": "refs",
  "cas_prefix": "cas/sha256"
}
```

Invariants:

- The descriptor is always present after constructing `RemoteOps`.
- If a descriptor exists but does not exactly match the expected descriptor object, it is overwritten to match.

### Public Exceptions (Remote)

These exception types are public because they are imported/used outside `remote.py` (e.g. tests):

- `daggerml_cli.ops.remote.RemoteError`
- `daggerml_cli.ops.remote.RefAlreadyExists`
- `daggerml_cli.ops.remote.InvalidOid`
- `daggerml_cli.ops.remote.InvalidManifest`
- `daggerml_cli.ops.remote.InvalidRef`
- `daggerml_cli.ops.remote.MissingCasObject`
- `daggerml_cli.ops.remote.ShaMismatch`

### Remote CAS Keys

- CAS objects are addressed by OID: a 64-char lowercase hex SHA256 digest.
- CAS key for OID `oid`:
  - Validates `oid` matches `^[0-9a-f]{64}$`, else raises `InvalidOid`.
  - Stores at: `{prefix}/cas/sha256/{aa}/{bb}/{oid}` where `aa=oid[:2]`, `bb=oid[2:4]`.

### Remote Ref Keys

- Remote refs are stored as JSON under `refs/`.
- Ref key for ref path `ref_path`:
  - Rejects paths starting with `/` or containing `..` (raises `ValueError`).
  - Stores at: `{prefix}/refs/{ref_path}`.

### Remote Ref JSON Format

```json
{
  "kind": "ref",
  "schema": 0,
  "target": "<manifest-oid>",
  "created_at": 1234567890,
  "meta": {}
}
```

Validation:

- `kind == "ref"`, `schema == 0`.
- `target` is a 64-char lowercase hex string.
- `created_at` is an integer.
- Invalid inputs raise `InvalidRef`.

Invariants:

- `target` always points to a remote manifest CAS object (by OID), not directly to a repo object.

### Remote Manifest JSON Format

```json
{
  "kind": "manifest",
  "schema": 0,
  "root-ns": "commit",
  "root-id": "<commit-id>",
  "closure": {
    "<namespace>": ["<oid>", "..."],
    "...": []
  }
}
```

Validation:

- `kind == "manifest"`, `schema == 0`.
- `root-ns` and `root-id` are present.
- `closure` is a dict of lists.
- Each list is sorted ascending and contains no duplicates.
- Each OID matches `^[0-9a-f]{64}$`.
- Invalid inputs raise `InvalidManifest`.

Invariants:

- `root-ns` is `"commit"` for all manifests produced/consumed by `RemoteOps.push()`/`RemoteOps.pull()`.

### RemoteOps Public Methods

- `RemoteOps.push(ref: Ref) -> str`
  - Requires: `ref.ns() == "commit"` (else `ValueError`).
  - Behavior:
    - Dumps a local manifest from the local DB for `ref`.
    - For every entry in local `closure`, base64-decodes the stored dump string and verifies SHA256 equals the object id; else `ShaMismatch`.
    - Uploads missing CAS objects.
    - Builds a remote manifest where closure maps `ns -> sorted([ids...])`.
    - Stores the manifest as a CAS object under its own SHA256 digest.
    - Writes a remote ref JSON at `refs/commits/<commit-id>.json`.
  - Errors:
    - `RefAlreadyExists` if the remote ref path already exists.
    - `ShaMismatch` on integrity failures.
  - Invariants:
    - Returned `ref_path` is always `commits/<commit-id>.json`.

- `RemoteOps.pull(ref_path: str) -> None`
  - Requires:
    - `ref_path` must be a valid remote ref path (see traversal checks).
    - The referenced manifest must have `root-ns == "commit"` else `ValueError`.
  - Behavior:
    - Fetches and validates the ref JSON.
    - Fetches and validates the manifest JSON.
    - Downloads missing CAS objects and verifies SHA256 integrity.
    - Constructs a local manifest with base64-encoded dumps for missing objects and loads it into the local DB.
    - Writes a local `head:` pointing at the pulled commit:
      - Head ref: `head:{remote_name}/{ref_path}` where `remote_name` is `s3://{bucket}` or `s3://{bucket}/{prefix}`.
  - Errors:
    - `RemoteError` if ref/manifest cannot be found.
    - `ShaMismatch` on integrity failures.
  - Invariants:
    - After success, the local DB contains `commit:<root-id>` from the pulled manifest.

- `RemoteOps.list(prefix: str) -> list[dict]`
  - Behavior: lists and decodes all `refs/<prefix>/*.json` remote refs; returns decoded ref objects augmented with `ref_path`.
  - Notes: malformed/unreadable refs are skipped.

- `RemoteOps.prune() -> int`
  - Behavior: deletes expired cache refs under `refs/cache/` where `ref.meta.cache.expires_at` is an int < now.
  - Returns: number deleted.

- `RemoteOps.gc(min_age_seconds: int = 24 * 3600) -> dict[str, int]`
  - Behavior:
    - Calls `prune()`.
    - Marks reachable OIDs from all `tags/`, `commits/`, and `cache/` refs and their manifests.
    - Sweeps CAS objects under `cas/sha256/` not live and older than `min_age_seconds`.
  - Returns: `{"deleted": n, "kept_live": n, "kept_young": n}`.
