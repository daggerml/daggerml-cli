"""Index operation CLI setup."""

from __future__ import annotations

import json
import re
from argparse import ArgumentParser
from typing import Any, Optional

from daggerml_cli._cli.base import apply_help_config, parse_ref
from daggerml_cli._db import Ref

_JSON_NUMBER_RE = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$")


def setup_index_parser(parser: ArgumentParser) -> None:
    """Setup index operation parsers and subcommands."""
    apply_help_config(
        parser,
        description=(
            "Index operations: create indexes, build DAGs, and commit results.\n\n"
            "Note: working with an active index (other than deleting it) is a known sharp edge "
            "and only partially implemented."
        ),
        examples=[
            "dml index list",
            "dml index create --head head:main",
            'dml index create --dump "<base64>"',
            'dml index start-fn index:abc node:fn node:arg1 --kwargv \'{"x":"node:arg2"}\' --no-cache',
            "dml index put-literal index:abc '{\"k\": true}' --name payload",
            'dml index commit index:abc node:result --head head:main --message "done" --dag-name output',
        ],
    )
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Methods", required=True)

    setup_index_list_parser(subparsers.add_parser("list", help="List indexes"))
    setup_index_start_fn_parser(subparsers.add_parser("start-fn", help="Start a function in an index"))
    setup_index_delete_parser(subparsers.add_parser("delete", help="Delete an index"))
    setup_index_create_parser(subparsers.add_parser("create", help="Create an index"))
    setup_index_get_kwargv_parser(subparsers.add_parser("get-kwargv", help="Get kwargv node for index"))
    setup_index_get_argv_parser(subparsers.add_parser("get-argv", help="Get argv node for index"))
    setup_index_put_import_parser(subparsers.add_parser("put-import", help="Import a node into an index"))
    setup_index_put_literal_parser(subparsers.add_parser("put-literal", help="Insert literal value into index"))
    setup_index_commit_parser(subparsers.add_parser("commit", help="Commit an index"))
    setup_index_dump_parser(subparsers.add_parser("dump", help="Dump a commit payload"))


def setup_index_list_parser(parser: ArgumentParser) -> None:
    """Setup index list command parser."""
    apply_help_config(parser, description="List indexes.", examples=["dml index list"])
    parser.set_defaults(op="index", method="list", func=execute_index_list)


def setup_index_start_fn_parser(parser: ArgumentParser) -> None:
    """Setup index start-fn command parser."""
    apply_help_config(
        parser,
        description="Start a function call in an index.",
        examples=[
            'dml index start-fn index:abc node:fn node:arg1 --kwargv \'{"x":"node:arg2"}\' --no-cache',
        ],
    )
    parser.add_argument("index_ref", help="Index ref (index:<id>)")
    parser.add_argument("argv", nargs="+", help="Node refs for argv (node:<id> ...) (positional list)")
    parser.add_argument("--kwargv", help="JSON object mapping names to node refs")
    parser.add_argument("--name", help="Name for the created node")
    parser.add_argument("--cache", dest="cache", action="store_true", help="Enable cache lookup")
    parser.add_argument("--no-cache", dest="cache", action="store_false", help="Disable cache lookup")
    parser.set_defaults(op="index", method="start-fn", func=execute_index_start_fn, cache=True)


def setup_index_delete_parser(parser: ArgumentParser) -> None:
    """Setup index delete command parser."""
    apply_help_config(parser, description="Delete an index.", examples=["dml index delete index:abc123"])
    parser.add_argument("index_ref", help="Index ref (index:<id>)")
    parser.set_defaults(op="index", method="delete", func=execute_index_delete)


def setup_index_create_parser(parser: ArgumentParser) -> None:
    """Setup index create command parser."""
    apply_help_config(
        parser,
        description="Create an index from a head ref or a dump payload.",
        examples=[
            "dml index create --head head:main",
            'dml index create --dump "<base64>"',
        ],
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--head", help="Head ref (head:<name>)")
    group.add_argument("--dump", help="Base64 dump payload")
    parser.set_defaults(op="index", method="create", func=execute_index_create)


def setup_index_get_kwargv_parser(parser: ArgumentParser) -> None:
    """Setup index get-kwargv command parser."""
    apply_help_config(
        parser, description="Get the kwargv node ref for an index.", examples=["dml index get-kwargv index:abc123"]
    )
    parser.add_argument("index_ref", help="Index ref (index:<id>)")
    parser.set_defaults(op="index", method="get-kwargv", func=execute_index_get_kwargv)


def setup_index_get_argv_parser(parser: ArgumentParser) -> None:
    """Setup index get-argv command parser."""
    apply_help_config(
        parser, description="Get the argv node ref for an index.", examples=["dml index get-argv index:abc123"]
    )
    parser.add_argument("index_ref", help="Index ref (index:<id>)")
    parser.set_defaults(op="index", method="get-argv", func=execute_index_get_argv)


def setup_index_put_import_parser(parser: ArgumentParser) -> None:
    """Setup index put-import command parser."""
    apply_help_config(
        parser,
        description="Import a DAG (and optional node) into an index.",
        examples=["dml index put-import index:abc123 dag:def456 --node node:abc --name imported"],
    )
    parser.add_argument("index_ref", help="Index ref (index:<id>)")
    parser.add_argument("dag_ref", help="DAG ref (dag:<id>)")
    parser.add_argument("--node", dest="node_ref", help="Optional node ref (node:<id>)")
    parser.add_argument("--name", help="Name for the imported node")
    parser.set_defaults(op="index", method="put-import", func=execute_index_put_import)


def setup_index_put_literal_parser(parser: ArgumentParser) -> None:
    """Setup index put-literal command parser."""
    apply_help_config(
        parser,
        description="Insert a literal value into an index (accepts JSON for objects/lists/primitives).",
        examples=["dml index put-literal index:abc123 '{\"k\": true}' --name payload"],
    )
    parser.add_argument("index_ref", help="Index ref (index:<id>)")
    parser.add_argument("value", help="Literal or JSON value")
    parser.add_argument("--name", help="Name for the literal node")
    parser.set_defaults(op="index", method="put-literal", func=execute_index_put_literal)


def setup_index_commit_parser(parser: ArgumentParser) -> None:
    """Setup index commit command parser."""
    apply_help_config(
        parser,
        description="Commit an index value to produce a commit ref (and optionally update a head).",
        examples=[
            'dml index commit index:abc node:result --head head:main --message "done" --dag-name output',
        ],
    )
    parser.add_argument("index_ref", help="Index ref (index:<id>)")
    parser.add_argument("value_ref", help="Node ref (node:<id>)")
    parser.add_argument("--head", help="Head ref (head:<name>)")
    parser.add_argument("--message", help="Commit message")
    parser.add_argument("--dag-name", help="DAG name to store in tree")
    parser.set_defaults(op="index", method="commit", func=execute_index_commit)


def setup_index_dump_parser(parser: ArgumentParser) -> None:
    """Setup index dump command parser."""
    apply_help_config(parser, description="Dump a commit payload.", examples=["dml index dump commit:abc123"])
    parser.add_argument("commit_ref", help="Commit ref (commit:<id>)")
    parser.set_defaults(op="index", method="dump", func=execute_index_dump)


def execute_index_list(ops, args) -> list[str]:
    """Execute index list command."""
    result = ops.list()
    return [ref.to for ref in result]


def execute_index_start_fn(ops, args) -> Optional[str]:
    """Execute index start-fn command."""
    index_ref = parse_ref(args.index_ref)
    argv = _parse_ref_list(args.argv)
    kwargv = _parse_kwargv(args.kwargv)
    result = ops.start_fn(index_ref, argv, kwargv, args.name, args.cache)
    return result.to if result is not None else None


def execute_index_delete(ops, args) -> None:
    """Execute index delete command."""
    index_ref = parse_ref(args.index_ref)
    ops.delete(index_ref)
    return None


def execute_index_create(ops, args) -> str:
    """Execute index create command."""
    head_ref = parse_ref(args.head) if args.head else None
    result = ops.create(head=head_ref, dump=args.dump)
    return result.to


def execute_index_get_kwargv(ops, args) -> str:
    """Execute index get-kwargv command."""
    index_ref = parse_ref(args.index_ref)
    result = ops.get_kwargv(index_ref)
    return result.to


def execute_index_get_argv(ops, args) -> str:
    """Execute index get-argv command."""
    index_ref = parse_ref(args.index_ref)
    result = ops.get_argv(index_ref)
    return result.to


def execute_index_put_import(ops, args) -> str:
    """Execute index put-import command."""
    index_ref = parse_ref(args.index_ref)
    dag_ref = parse_ref(args.dag_ref)
    node_ref = parse_ref(args.node_ref) if args.node_ref else None
    result = ops.put_import(index_ref, dag_ref, node_ref, args.name)
    return result.to


def execute_index_put_literal(ops, args) -> str:
    """Execute index put-literal command."""
    index_ref = parse_ref(args.index_ref)
    value = _parse_json_value(args.value)
    result = ops.put_literal(index_ref, value, name=args.name)
    return result.to


def execute_index_commit(ops, args) -> str:
    """Execute index commit command."""
    index_ref = parse_ref(args.index_ref)
    value_ref = parse_ref(args.value_ref)
    head_ref = parse_ref(args.head) if args.head else None
    result = ops.commit(index_ref, value_ref, head=head_ref, message=args.message, dag_name=args.dag_name)
    return result.to


def execute_index_dump(ops, args) -> str:
    """Execute index dump command."""
    commit_ref = parse_ref(args.commit_ref)
    result = ops.dump(commit_ref)
    return result


def _parse_json_value(value: str) -> Any:
    """Parse JSON values or return the raw string."""
    raw = value.strip()
    if not raw:
        return value
    looks_like_json = raw[0] in ("{", "[") or raw in {"true", "false", "null"} or _JSON_NUMBER_RE.match(raw)
    if not looks_like_json:
        return value
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON value: {value}") from exc


def _parse_ref_list(values: list[str]) -> list[Ref]:
    """Parse list of references into Ref objects."""
    return [parse_ref(value) for value in values]


def _parse_kwargv(value: str | None) -> Optional[dict[str, Ref]]:
    """Parse kwargv JSON values into ref map."""
    if value is None:
        return None
    try:
        data = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid JSON for --kwargv") from exc
    if not isinstance(data, dict):
        raise ValueError("--kwargv must be a JSON object")
    result: dict[str, Ref] = {}
    for key, ref_value in data.items():
        if not isinstance(ref_value, str):
            raise ValueError("--kwargv values must be ref strings")
        result[key] = parse_ref(ref_value)
    return result
