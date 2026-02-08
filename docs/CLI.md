---
slug: cli
---

# CLI (`dml`)

This document specifies the public contract of the `dml` command-line interface implemented by `src/daggerml_cli/_cli/`.

## Scope

- Covers the CLI surface for end users: commands, flags, environment variables, stdout/stderr behavior, JSON formats, and exit behavior.
- Covers the Python surfaces used across modules (and by tests) inside `src/daggerml_cli/_cli/`.
- Does not specify the semantics of underlying repo operations beyond what is observable via this CLI (those belong to the ops layer).

## Public Surface Rule (enforced here)

Any interface used outside its defining module/file is public. This includes:

- CLI commands/flags/env vars
- Python types/functions imported across `_cli` modules
- Helper functions imported by `tests/cli/*`

Downstream work MUST NOT infer missing details for any public surface listed here.

## User-Facing CLI Contract

### Command name and entrypoint

- Installed console script name: `dml`
- Python entrypoint: `src/daggerml_cli/_cli/__init__.py:cli()`

### Global flags and environment

`dml [--repo PATH] [-v|-vv|-vvv...] <operation> ...`

Global flags:

- `--repo PATH` (string, optional)
  - If provided, it is used as the repository path.
  - If omitted, `DML_REPO_PATH` is used when set.
  - If neither is set, the current working directory is used.
- `-v, --verbose` (counting flag, optional)
  - `0` (default): logging level WARNING
  - `1`: logging level INFO
  - `>=2`: logging level DEBUG

Environment variables:

- `DML_REPO_PATH` (string): default repository path when `--repo` is not provided.
- `DML_S3_BUCKET` (string): default S3 bucket for `dml remote ...` when `--s3-bucket` is not provided.
- `DML_S3_PREFIX` (string): default S3 prefix for `dml remote ...` when `--s3-prefix` is not provided (defaults to empty string).

### Stdout/stderr and JSON

Success output:

- Exactly one JSON value is written to stdout via `json.dump(..., separators=(",", ":"))`, followed by a single `\n`.
- Unless `-v` is used, stderr SHOULD be empty on success.

Error output:

- Runtime exceptions are caught by `src/daggerml_cli/_cli/base.py:execute_command()`.
- A single JSON object is written to stderr, followed by a single `\n`.
- If verbose logging is enabled, additional non-JSON log lines MAY appear on stderr; the error JSON line is still emitted.

JSON encoding rules:

- `daggerml_cli._db.Ref` instances are encoded as `str(ref)`.
  - For `Ref`, `str(ref)` is its `__repr__` form: `"Ref(<to>)"`.
- `daggerml_cli._db.Resource` instances are encoded as:

```json
{
  "__type__": "resource",
  "uri": "<uri>",
  "data": "<str(resource.data)>" | null,
  "adapter": "<adapter>" | null
}
```

### Exit behavior

- For runtime errors handled by `execute_command()`, the process exit code is NOT set by the CLI (i.e., it will typically remain `0`).
- For argument parsing errors or `--help`, `argparse` raises `SystemExit`:
  - `--help` exits with code `0` after printing help.
  - Parse errors (including missing required subcommands where configured) exit with code `2`.

### Error payload schema

On runtime error, stderr JSON matches:

```json
{
  "error": "<message>",
  "type": "<ExceptionClassName>",
  "command": "<command context>"  // present only when context is available
}
```

Additional rules:

- When `command` is present, `error` is prefixed with `"<command>: "`.
- Ref-format errors are normalized to mention `namespace:id`.
- Repo path errors include a recovery hint to pass `--repo` or set `DML_REPO_PATH`.

### Operations and methods

All commands open the repository with:

- `with daggerml_cli.DmlOps.open(get_repo_path(...)) as ops:`

and invoke a handler `args.func(ops_obj, args)`.

#### `dml commit <method>`

Parser setup: `src/daggerml_cli/_cli/commit.py:setup_commit_parser()`.

- `dml commit list <head> [--limit N]`
  - Inputs:
    - `head` (string): passed through `parse_ref(head)`.
    - `--limit N` (int, optional): passed through to `ops_obj.list`.
  - Behavior: calls `ops_obj.list(head_ref, limit)`.
  - Output (stdout): JSON array of strings; each element is `str(ref)` for returned commit refs.
- `dml commit merge <commit1> <commit2> --user USER`
  - Behavior: calls `ops_obj.merge(parse_ref(commit1), parse_ref(commit2), user)`.
  - Output: JSON string `str(result)`.
- `dml commit rebase <source> <target> --user USER`
  - Behavior: calls `ops_obj.rebase(parse_ref(source), parse_ref(target), user)`.
  - Output: JSON string `str(result)`.
- `dml commit get-dag <commit> <name>`
  - Behavior: calls `ops_obj.get_dag(parse_ref(commit), name)`.
  - Output: JSON string `str(result)` if non-null, else JSON `null`.
- `dml commit delete-dag <name> <head> --user USER`
  - Behavior: calls `ops_obj.delete_dag(name, parse_ref(head), user)`.
  - Output: JSON string `str(result)`.

#### `dml head <method>`

Parser setup: `src/daggerml_cli/_cli/head.py:setup_head_parser()`.

- `dml head list`
  - Behavior: calls `head_ops.list()`.
  - Output: JSON array of strings; each element is `str(ref)`.
- `dml head create <branch_name> [--from FROM]`
  - Inputs:
    - `branch_name` (string): passed to `head_ops.create`.
    - `--from FROM` (string, optional):
      - If it contains no `:`, it is rewritten to `head:<FROM>`.
      - Then passed through `parse_ref`.
  - Behavior:
    - Calls `head_ops.create(branch_name, parsed_from_or_none)`.
    - Opens a read-only transaction via `head_ops._tx(readonly=True)` and reads the created head object via `txn.get(result)`.
  - Output (object):

```json
{"ref":"<head-ref>","commit":"<commit-ref>"}
```

- `dml head delete <head_ref>`
  - Behavior:
    - `parse_ref(head_ref)`.
    - Validates `parsed_head.to.startswith("head:")`; otherwise raises `ValueError("Head reference must start with 'head:'")`.
    - Calls `head_ops.delete(parsed_head)`.
  - Output: JSON `null`.

#### `dml index <method>`

Parser setup: `src/daggerml_cli/_cli/index.py:setup_index_parser()`.

- `dml index list`
  - Output: JSON array of strings (each is `ref.to` for returned index refs).
- `dml index start-fn <index_ref> <argv...> [--kwargv JSON] [--name NAME] [--cache|--no-cache]`
  - Inputs:
    - `index_ref` (string): `parse_ref`.
    - `argv` (list[string], 1+): each `parse_ref`.
    - `--kwargv` (string, optional): JSON object mapping string keys to ref strings.
      - If not valid JSON: raises `ValueError("Invalid JSON for --kwargv")` (with `json.JSONDecodeError` as cause).
      - If decoded value is not an object: raises `ValueError("--kwargv must be a JSON object")`.
      - If any value is not a string: raises `ValueError("--kwargv values must be ref strings")`.
      - Values are converted with `parse_ref`.
    - `--cache` / `--no-cache`:
      - default is cache enabled (`True`)
  - Behavior: calls `ops.start_fn(index_ref, argv_refs, kwargv_or_none, name_or_none, cache_bool)`.
  - Output: JSON string `result.to` if non-null, else JSON `null`.
- `dml index delete <index_ref>`
  - Output: JSON `null`.
- `dml index create (--head head:<name> | --dump <base64>)`
  - Mutual exclusion is enforced by argparse.
  - Output: JSON string `result.to`.
- `dml index get-kwargv <index_ref>`
  - Output: JSON string `result.to`.
- `dml index get-argv <index_ref>`
  - Output: JSON string `result.to`.
- `dml index put-import <index_ref> <dag_ref> [--node node_ref] [--name NAME]`
  - Output: JSON string `result.to`.
- `dml index put-literal <index_ref> <value> [--name NAME]`
  - Input `value` parsing:
    - If `value` appears to be JSON (object/array, `true`/`false`/`null`, or a JSON number), it is parsed with `json.loads`.
    - On JSON decode error: raises `ValueError(f"Invalid JSON value: {value}")` with the JSON error as cause.
    - Otherwise, `value` is passed as a raw string.
  - Output: JSON string `result.to`.
- `dml index commit <index_ref> <value_ref> [--head head:<name>] [--message MSG] [--dag-name NAME]`
  - Output: JSON string `result.to`.
- `dml index dump <commit_ref>`
  - Output: JSON string (opaque dump payload returned by `ops.dump`).

#### `dml cache <method>`

Parser setup: `src/daggerml_cli/_cli/cache.py:setup_cache_parser()`.

- `dml cache put <dag_ref>`
  - Output: JSON string `result.to`.
- `dml cache get <argspec_ref>`
  - Output: JSON string `result.to` if non-null, else JSON `null`.
- `dml cache delete <argspec_ref>`
  - Output: JSON boolean.
- `dml cache list [--limit N]`
  - `--limit` parsing:
    - Must be a positive integer (>0), else argparse fails with `ArgumentTypeError("limit must be a positive integer")`.
  - Output: JSON array of 2-tuples encoded as 2-element JSON arrays:

```json
[
  ["<cache-ref>", {"dag": "<dag-ref>"}]
]
```

- `dml cache clear`
  - Output: JSON integer (count removed).

#### `dml dag <method>`

Parser setup: `src/daggerml_cli/_cli/dag.py:setup_dag_parser()`.

- `dml dag list`
  - Output: JSON array of objects (shape defined by `ops_obj.list()`).
- `dml dag describe <dag_ref>`
  - Output: JSON object (shape defined by `ops_obj.describe`).
- `dml dag get-node <dag_ref> <name>`
  - Output: JSON string `result.to`.
- `dml dag get-argv <dag_ref>`
  - Output: JSON string `result.to`.
- `dml dag get-kwargv <dag_ref>`
  - Output: JSON string `result.to`.

#### `dml node <method>`

Parser setup: `src/daggerml_cli/_cli/node.py:setup_node_parser()`.

- `dml node get <node_ref>`
  - Output: JSON value returned by `ops_obj.get(parse_ref(node_ref))`, encoded with `DmlJsonEncoder`.
- `dml node unroll <node_ref>`
  - Output: JSON value returned by `ops_obj.unroll(parse_ref(node_ref))`, encoded with `DmlJsonEncoder`.

#### `dml gc <method>`

Parser setup: `src/daggerml_cli/_cli/gc.py:setup_gc_parser()`.

- `dml gc run`
  - Output: JSON object `dict[str,int]`.
- `dml gc list-orphans [--heads <ref> ...]`
  - `--heads` behavior:
    - If omitted: `heads` is `null` (Python `None`) and passed through to `ops.list_orphans(None)`.
    - If present with no values (`--heads`): `heads` is an empty list.
    - Each provided value is passed through `parse_ref`, then validated by calling `ref.ns()`.
  - Output: JSON array of strings representing `Ref` objects encoded as `"Ref(<to>)"`.

#### `dml remote <method>`

Parser setup: `src/daggerml_cli/_cli/remote.py:setup_remote_parser()`.

Remote configuration:

- `--s3-bucket BUCKET` (string, optional; required unless `DML_S3_BUCKET` set)
- `--s3-prefix PREFIX` (string, optional; defaults to `DML_S3_PREFIX` or empty string)

Lazy boto3 requirement:

- `boto3` is imported only during execution of remote handlers via `require_boto3()`.
- If `boto3` is missing, remote handlers raise `daggerml_cli.types.DmlRepoError` with message:
  - `"Remote commands require boto3; install boto3 to continue"`

Ops resolution quirk (public behavior):

- Remote subcommands set `args.op = "commit"` intentionally.
- Therefore the command context in error payloads for `dml remote ...` is `"commit <method>"` (not `"remote <method>"`).

Commands:

- `dml remote push <ref> --s3-bucket BUCKET [--s3-prefix PREFIX]`
  - `ref` is passed through `parse_ref`.
  - Output: JSON string returned by `RemoteOps.push` (a remote ref path).
- `dml remote pull <ref_path> --s3-bucket BUCKET [--s3-prefix PREFIX]`
  - Output: JSON `null`.
- `dml remote list <prefix> --s3-bucket BUCKET [--s3-prefix PREFIX]`
  - Output: JSON array of objects (shape defined by `RemoteOps.list`).
- `dml remote prune --s3-bucket BUCKET [--s3-prefix PREFIX]`
  - Output: JSON integer (count deleted).
- `dml remote gc [--min-age SECONDS] --s3-bucket BUCKET [--s3-prefix PREFIX]`
  - `--min-age` default: `86400` (24 hours).
  - Output: JSON object `dict[str,int]` (shape defined by `RemoteOps.gc`).

## Public Python Surface (within `daggerml_cli._cli`)

This section is normative for downstream code/tests importing these symbols.

### `src/daggerml_cli/_cli/__init__.py`

- `cli() -> None`
  - Parses `sys.argv`.
  - Configures logging via `base.setup_logging(args.verbose)`.
  - If `args.op` is set, calls `base.execute_command(args)`.
  - If `args.op` is not set (no operation provided), prints help to stdout.

### `src/daggerml_cli/_cli/base.py`

- `class DmlJsonEncoder(json.JSONEncoder)`
  - `default(self, obj) -> Any` implements JSON encoding rules for `Ref` and `Resource` as described above.
- `get_repo_path(repo_arg: str | None) -> str`
  - Implements the `--repo` / `DML_REPO_PATH` / `cwd` resolution described above.
- `get_ops_object(ops: daggerml_cli.DmlOps, op_name: str) -> Any`
  - Returns `getattr(ops, op_name)`.
  - Raises `AttributeError` if `op_name` is not present.
- `parse_ref(ref_string: str) -> daggerml_cli._db.Ref`
  - Returns `Ref(ref_string)` without validating the `namespace:id` shape.
- `setup_logging(verbose_level: int) -> None`
  - Calls `logging.basicConfig(level=..., stream=sys.stderr, format="%(levelname)s: %(message)s")`.
- `output_json(data: Any) -> None`
  - Writes compact JSON to stdout with a trailing newline.
- `build_help_epilog(examples: Sequence[str]) -> str`
  - Returns a multi-line string starting with `"Examples:"`, or `""` if there are no examples.
- `apply_help_config(parser: argparse.ArgumentParser, *, description: str, examples: Sequence[str] | None = None) -> None`
  - Sets raw-description formatter, assigns description, and optional epilog produced by `build_help_epilog`.
- `normalize_error_message(error: Exception, *, command: str | None) -> str`
  - Normalizes invalid-ref messages to mention `namespace:id`.
  - Adds repo-path recovery hints for path-related exceptions.
  - Includes `"<command>: "` prefix when `command` is provided.
- `build_error_payload(error: Exception, *, command: str | None) -> dict[str, str]`
  - Builds the JSON error payload schema described above.
- `output_error(error: Exception, command: str | None = None) -> None`
  - Writes the structured JSON error payload to stderr with a trailing newline.
- `execute_command(args: Any) -> None`
  - Requires `args.func`.
  - Opens repo via `DmlOps.open(get_repo_path(args.repo))`.
  - Resolves `ops_obj = getattr(ops, args.op)`.
  - Calls `args.func(ops_obj, args)` and writes result via `output_json`.
  - On any exception, writes error via `output_error` and returns.

### `src/daggerml_cli/_cli/remote.py` (additional public helpers)

- `resolve_s3_config(args: argparse.Namespace, env: Mapping[str, str] = os.environ) -> tuple[str, str]`
  - Returns `(bucket, prefix)`; `prefix` defaults to `""`.
  - Raises `DmlRepoError` if bucket is missing.
- `require_boto3() -> Any`
  - Imports and returns the `boto3` module.
  - Raises `DmlRepoError` on ImportError.
- `create_s3_client(boto3_module: Any) -> Any`
  - Returns `boto3_module.client("s3")`.
- `get_remote_ops(ops: Any, s3_client: Any, *, bucket: str, prefix: str) -> daggerml_cli.ops.remote.RemoteOps`
  - Requires `ops._db` to be present.
  - Returns `RemoteOps(_db=ops._db, client=s3_client, bucket=bucket, prefix=prefix)`.

### Operation modules: public setup + handlers

The following functions are imported by `src/daggerml_cli/_cli/__init__.py` and/or `tests/cli/*` and are therefore public.

`src/daggerml_cli/_cli/commit.py`

- `setup_commit_parser(parser: argparse.ArgumentParser) -> None`
- `execute_commit_list(ops_obj: Any, args: Any) -> list[str]`
- `execute_commit_merge(ops_obj: Any, args: Any) -> str`
- `execute_commit_rebase(ops_obj: Any, args: Any) -> str`
- `execute_commit_get_dag(ops_obj: Any, args: Any) -> str | None`
- `execute_commit_delete_dag(ops_obj: Any, args: Any) -> str`

`src/daggerml_cli/_cli/head.py`

- `setup_head_parser(parser: argparse.ArgumentParser) -> None`
- `execute_head_list(head_ops: Any, args: Any) -> list[str]`
- `execute_head_create(head_ops: Any, args: Any) -> dict[str, str]`
- `execute_head_delete(head_ops: Any, args: Any) -> None`

`src/daggerml_cli/_cli/index.py`

- `setup_index_parser(parser: argparse.ArgumentParser) -> None`
- `execute_index_list(ops: Any, args: Any) -> list[str]`
- `execute_index_start_fn(ops: Any, args: Any) -> str | None`
- `execute_index_delete(ops: Any, args: Any) -> None`
- `execute_index_create(ops: Any, args: Any) -> str`
- `execute_index_get_kwargv(ops: Any, args: Any) -> str`
- `execute_index_get_argv(ops: Any, args: Any) -> str`
- `execute_index_put_import(ops: Any, args: Any) -> str`
- `execute_index_put_literal(ops: Any, args: Any) -> str`
- `execute_index_commit(ops: Any, args: Any) -> str`
- `execute_index_dump(ops: Any, args: Any) -> str`

`src/daggerml_cli/_cli/cache.py`

- `setup_cache_parser(parser: argparse.ArgumentParser) -> None`
- `execute_cache_put(ops_obj: Any, args: Any) -> str`
- `execute_cache_get(ops_obj: Any, args: Any) -> str | None`
- `execute_cache_delete(ops_obj: Any, args: Any) -> bool`
- `execute_cache_list(ops_obj: Any, args: Any) -> list[list[Any]]`
- `execute_cache_clear(ops_obj: Any, args: Any) -> int`

`src/daggerml_cli/_cli/dag.py`

- `setup_dag_parser(parser: argparse.ArgumentParser) -> None`
- `execute_dag_list(ops_obj: Any, args: Any) -> list[dict[str, Any]]`
- `execute_dag_describe(ops_obj: Any, args: Any) -> dict[str, Any]`
- `execute_dag_get_node(ops_obj: Any, args: Any) -> str`
- `execute_dag_get_argv(ops_obj: Any, args: Any) -> str`
- `execute_dag_get_kwargv(ops_obj: Any, args: Any) -> str`

`src/daggerml_cli/_cli/node.py`

- `setup_node_parser(parser: argparse.ArgumentParser) -> None`
- `execute_node_get(ops_obj: Any, args: Any) -> Any`
- `execute_node_unroll(ops_obj: Any, args: Any) -> Any`

`src/daggerml_cli/_cli/gc.py`

- `setup_gc_parser(parser: argparse.ArgumentParser) -> None`
- `execute_gc_run(ops: Any, args: Any) -> dict[str, int]`
- `execute_gc_list_orphans(ops: Any, args: Any) -> list[daggerml_cli._db.Ref]`
- `parse_heads(heads: list[str] | None) -> list[daggerml_cli._db.Ref] | None`

`src/daggerml_cli/_cli/remote.py`

- `setup_remote_parser(parser: argparse.ArgumentParser) -> None`
- `execute_remote_push(ops: Any, args: Any) -> str`
- `execute_remote_pull(ops: Any, args: Any) -> None`
- `execute_remote_list(ops: Any, args: Any) -> list[dict]`
- `execute_remote_prune(ops: Any, args: Any) -> int`
- `execute_remote_gc(ops: Any, args: Any) -> dict[str, int]`
