from __future__ import annotations

import argparse
import importlib
import os
from argparse import ArgumentParser
from typing import Any, Mapping

from daggerml_cli import DmlOps
from daggerml_cli._cli.base import apply_help_config, parse_ref
from daggerml_cli.types import DmlRepoError


def setup_remote_parser(parser: ArgumentParser) -> None:
    """Setup remote operation parsers and subcommands."""
    apply_help_config(
        parser,
        description="Remote operations backed by S3 (requires boto3 at runtime for remote commands).",
        examples=[
            "dml remote push commit:<id> --s3-bucket BUCKET [--s3-prefix PREFIX]",
            "dml remote pull commits/<id>.json --s3-bucket BUCKET",
        ],
    )
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Methods", required=True)

    setup_remote_push_parser(subparsers.add_parser("push", help="Push a commit ref to remote"))
    setup_remote_pull_parser(subparsers.add_parser("pull", help="Pull a ref path from remote"))
    setup_remote_list_parser(subparsers.add_parser("list", help="List remote refs under a prefix"))
    setup_remote_prune_parser(subparsers.add_parser("prune", help="Prune expired remote cache refs"))
    setup_remote_gc_parser(subparsers.add_parser("gc", help="Run remote garbage collection"))


def setup_remote_push_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote push`."""
    apply_help_config(
        parser,
        description="Push a commit ref to remote storage.",
        examples=["dml remote push commit:abc123 --s3-bucket BUCKET --s3-prefix PREFIX"],
    )
    parser.add_argument("ref", help="Commit ref (commit:<id>)")
    _add_s3_args(parser)
    parser.set_defaults(method="push", func=execute_remote_push)


def setup_remote_pull_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote pull`."""
    apply_help_config(
        parser,
        description="Pull a ref path from remote storage into the local repo.",
        examples=["dml remote pull commits/abc123.json --s3-bucket BUCKET --s3-prefix PREFIX"],
    )
    parser.add_argument("ref_path", help="Remote ref path (e.g. commits/<id>.json)")
    _add_s3_args(parser)
    parser.set_defaults(method="pull", func=execute_remote_pull)


def setup_remote_list_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote list`."""
    apply_help_config(
        parser,
        description="List remote refs under a prefix.",
        examples=["dml remote list commits --s3-bucket BUCKET --s3-prefix PREFIX"],
    )
    parser.add_argument("prefix", help="Remote prefix to list (e.g. commits, tags, cache)")
    _add_s3_args(parser)
    parser.set_defaults(method="list", func=execute_remote_list)


def setup_remote_prune_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote prune`."""
    apply_help_config(
        parser,
        description="Prune expired remote cache refs.",
        examples=["dml remote prune --s3-bucket BUCKET --s3-prefix PREFIX"],
    )
    _add_s3_args(parser)
    parser.set_defaults(method="prune", func=execute_remote_prune)


def setup_remote_gc_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote gc`."""
    apply_help_config(
        parser,
        description="Run remote garbage collection.",
        examples=["dml remote gc --min-age 0 --s3-bucket BUCKET --s3-prefix PREFIX"],
    )
    parser.add_argument(
        "--min-age",
        type=int,
        default=24 * 3600,
        help="Minimum age in seconds for CAS objects to be eligible for deletion",
    )
    _add_s3_args(parser)
    parser.set_defaults(method="gc", func=execute_remote_gc)


def resolve_s3_config(
    args: argparse.Namespace,
    env: Mapping[str, str] = os.environ,
) -> tuple[str, str]:
    """Resolve S3 bucket and prefix from flags with environment fallback."""
    bucket = getattr(args, "s3_bucket", None) or env.get("DML_S3_BUCKET")
    prefix = getattr(args, "s3_prefix", None) or env.get("DML_S3_PREFIX") or ""
    if not bucket:
        raise DmlRepoError("S3 bucket required; pass --s3-bucket or set DML_S3_BUCKET")
    return bucket, prefix


def require_boto3() -> Any:
    """Import boto3 only when a remote command executes."""
    try:
        return importlib.import_module("boto3")
    except ImportError as exc:
        raise DmlRepoError("Remote commands require boto3; install boto3 to continue") from exc


def create_s3_client(boto3_module: Any) -> Any:
    """Create a boto3 S3 client using default credential resolution."""
    return boto3_module.client("s3")


def get_remote_ops(ops: DmlOps, s3_client: Any, *, bucket: str, prefix: str) -> Any:
    """Get remote operations from DmlOps instance."""
    return ops.remote(bucket=bucket, prefix=prefix, client=s3_client)


def execute_remote_push(ops, args) -> str:
    """Execute `dml remote push`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    bucket, prefix = resolve_s3_config(args)
    remote_ops = get_remote_ops(ops, s3_client, bucket=bucket, prefix=prefix)
    return remote_ops.push(parse_ref(args.ref))


def execute_remote_pull(ops, args) -> None:
    """Execute `dml remote pull`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    bucket, prefix = resolve_s3_config(args)
    remote_ops = get_remote_ops(ops, s3_client, bucket=bucket, prefix=prefix)
    remote_ops.pull(args.ref_path)
    return None


def execute_remote_list(ops, args) -> list[dict]:
    """Execute `dml remote list`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    bucket, prefix = resolve_s3_config(args)
    remote_ops = get_remote_ops(ops, s3_client, bucket=bucket, prefix=prefix)
    return remote_ops.list(args.prefix)


def execute_remote_prune(ops, args) -> int:
    """Execute `dml remote prune`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    bucket, prefix = resolve_s3_config(args)
    remote_ops = get_remote_ops(ops, s3_client, bucket=bucket, prefix=prefix)
    return remote_ops.prune()


def execute_remote_gc(ops, args) -> dict[str, int]:
    """Execute `dml remote gc`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    bucket, prefix = resolve_s3_config(args)
    remote_ops = get_remote_ops(ops, s3_client, bucket=bucket, prefix=prefix)
    return remote_ops.gc(min_age_seconds=args.min_age)


def _add_s3_args(parser: ArgumentParser) -> None:
    parser.add_argument("--s3-bucket", dest="s3_bucket", help="S3 bucket name")
    parser.add_argument("--s3-prefix", dest="s3_prefix", help="S3 prefix (optional)")
