"""Unit and integration tests for remote CLI functionality."""

from __future__ import annotations

import builtins
import importlib
import json
import os
import shutil
import sys
import tempfile
from argparse import ArgumentParser, Namespace
from io import StringIO

import pytest

from daggerml_cli import DmlOps
from daggerml_cli._cli import cli
from daggerml_cli._cli.remote import require_boto3, resolve_s3_config, setup_remote_parser
from daggerml_cli._db import Ref
from daggerml_cli.ops.base_ops import BaseOps
from daggerml_cli.types import DmlRepoError


class TestSetupRemoteParser:
    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_remote_parser(parser)

        args = parser.parse_args(["push", "commit:abc"])
        assert args.method == "push"

        args = parser.parse_args(["pull", "commits/abc.json"])
        assert args.method == "pull"

        args = parser.parse_args(["list", "commits"])
        assert args.method == "list"

        args = parser.parse_args(["prune"])
        assert args.method == "prune"

        args = parser.parse_args(["gc"])
        assert args.method == "gc"

    def test_s3_args_present(self):
        parser = ArgumentParser()
        setup_remote_parser(parser)
        args = parser.parse_args(["list", "commits", "--s3-bucket", "b", "--s3-prefix", "p"])
        assert args.s3_bucket == "b"
        assert args.s3_prefix == "p"


class TestResolveS3Config:
    def test_flag_overrides_env(self):
        args = Namespace(s3_bucket="flag-bucket", s3_prefix="flag-prefix")
        bucket, prefix = resolve_s3_config(args, env={"DML_S3_BUCKET": "env-bucket", "DML_S3_PREFIX": "env-prefix"})
        assert bucket == "flag-bucket"
        assert prefix == "flag-prefix"

    def test_env_fallback(self):
        args = Namespace(s3_bucket=None, s3_prefix=None)
        bucket, prefix = resolve_s3_config(args, env={"DML_S3_BUCKET": "env-bucket"})
        assert bucket == "env-bucket"
        assert prefix == ""

    def test_missing_bucket_raises(self):
        args = Namespace(s3_bucket=None, s3_prefix=None)
        with pytest.raises(DmlRepoError, match="--s3-bucket|DML_S3_BUCKET"):
            resolve_s3_config(args, env={})


class TestRequireBoto3:
    def test_missing_boto3_raises_dm_repo_error(self, monkeypatch):
        def _boom(_name: str):
            raise ImportError("nope")

        monkeypatch.setattr(importlib, "import_module", _boom)
        with pytest.raises(DmlRepoError, match="Remote commands require boto3; install boto3 to continue"):
            require_boto3()


def test_non_remote_commands_do_not_import_boto3(monkeypatch, tmp_path):
    # Ensure boto3 isn't already present (it may be imported by other tests).
    for mod in ["boto3", "daggerml_cli.ops.remote"]:
        if mod in sys.modules:
            monkeypatch.delitem(sys.modules, mod, raising=False)

    orig_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "boto3" or name.startswith("boto3."):
            raise ImportError("blocked boto3 import")
        return orig_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    old_argv = sys.argv
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.argv = ["dml", "--repo", str(tmp_path), "cache", "list"]
    sys.stdout = StringIO()
    sys.stderr = StringIO()
    try:
        cli()
    except SystemExit:
        # CLI uses sys.exit for some errors; the important part is boto3 wasn't imported.
        pass
    finally:
        sys.argv = old_argv
        sys.stdout = old_stdout
        sys.stderr = old_stderr


class TestRemoteCLIIntegration:
    def setup_method(self):
        self.repo_dir = tempfile.mkdtemp()

        # Create a repo with at least one commit so push works.
        dml_ops = DmlOps.open(self.repo_dir)
        head_ref = dml_ops.head().create("main")
        base_ops = BaseOps(dml_ops._db)
        with base_ops._tx(readonly=True) as txn:
            self.commit_ref = txn.get(head_ref).commit
        dml_ops.close()

    def teardown_method(self):
        shutil.rmtree(self.repo_dir)

    def _run_cli(self, repo_path: str, args: list[str]):
        import sys

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.argv = ["dml", "--repo", repo_path] + args
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        sys.stdout = stdout_capture
        sys.stderr = stderr_capture
        try:
            cli()
            return stdout_capture.getvalue(), stderr_capture.getvalue()
        except SystemExit:
            return stdout_capture.getvalue(), stderr_capture.getvalue()
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def test_remote_push_pull_list_prune_gc(self, s3, tmp_path):
        # Use short bucket/prefix so the local head ref written by RemoteOps.pull
        # stays within Ref format constraints.
        bucket = "bbb"
        prefix = "p"
        try:
            s3.create_bucket(Bucket=bucket)
        except Exception:
            pass

        # push
        stdout, stderr = self._run_cli(
            self.repo_dir,
            ["remote", "push", self.commit_ref.to, "--s3-bucket", bucket, "--s3-prefix", prefix],
        )
        assert not stderr
        ref_path = json.loads(stdout.strip())
        assert ref_path == f"commits/{self.commit_ref.id()}.json"

        # Create a short alias ref path for pull.
        pull_ref_path = "commits/x.json"
        src_key = f"{prefix}/refs/{ref_path}"
        dst_key = f"{prefix}/refs/{pull_ref_path}"
        ref_bytes = s3.get_object(Bucket=bucket, Key=src_key)["Body"].read()
        s3.put_object(Bucket=bucket, Key=dst_key, Body=ref_bytes)

        # list
        stdout, stderr = self._run_cli(
            self.repo_dir,
            ["remote", "list", "commits", "--s3-bucket", bucket, "--s3-prefix", prefix],
        )
        assert not stderr
        refs = json.loads(stdout.strip())
        assert any(r.get("ref_path") == ref_path for r in refs)

        # pull into a second repo
        other_repo = str(tmp_path / "other")
        os.makedirs(other_repo, exist_ok=True)
        DmlOps.open(other_repo).close()
        stdout, stderr = self._run_cli(
            other_repo,
            ["remote", "pull", pull_ref_path, "--s3-bucket", bucket, "--s3-prefix", prefix],
        )
        assert not stderr
        assert json.loads(stdout.strip()) is None

        # verify the remote head pointer was written
        remote_name = f"s3://{bucket}" + (f"/{prefix}" if prefix else "")
        remote_head_ref = Ref(f"head:{remote_name}/{pull_ref_path}")
        other_ops = DmlOps.open(other_repo)
        other_base = BaseOps(other_ops._db)
        with other_base._tx(readonly=True) as txn:
            head_obj = txn.get(remote_head_ref)
        other_ops.close()
        assert head_obj.commit.id() == self.commit_ref.id()

        # create an expired cache ref for prune
        expired_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "0" * 64,
            "created_at": 0,
            "meta": {"cache": {"expires_at": 0}},
        }
        expired_key = f"{prefix}/refs/cache/expired.json"
        s3.put_object(
            Bucket=bucket,
            Key=expired_key,
            Body=json.dumps(expired_ref, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )
        stdout, stderr = self._run_cli(
            self.repo_dir,
            ["remote", "prune", "--s3-bucket", bucket, "--s3-prefix", prefix],
        )
        assert not stderr
        assert json.loads(stdout.strip()) == 1

        # create an unreferenced CAS object for GC
        oid = "f" * 64
        aa, bb = oid[:2], oid[2:4]
        cas_key = f"{prefix}/cas/sha256/{aa}/{bb}/{oid}"
        s3.put_object(Bucket=bucket, Key=cas_key, Body=b"junk")
        stdout, stderr = self._run_cli(
            self.repo_dir,
            ["remote", "gc", "--min-age", "0", "--s3-bucket", bucket, "--s3-prefix", prefix],
        )
        assert not stderr
        result = json.loads(stdout.strip())
        assert result["deleted"] >= 1
