"""Unit and integration tests for index CLI functionality."""

import json
import tempfile
from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

import pytest

from daggerml_cli import DmlOps
from daggerml_cli._cli.index import (
    _parse_json_value,
    _parse_kwargv,
    _parse_ref_list,
    execute_index_commit,
    execute_index_create,
    execute_index_delete,
    execute_index_dump,
    execute_index_get_argv,
    execute_index_get_kwargv,
    execute_index_list,
    execute_index_put_import,
    execute_index_put_literal,
    execute_index_start_fn,
    setup_index_parser,
)
from daggerml_cli._db import Ref, Resource
from daggerml_cli.ops.base_ops import BaseOps
from daggerml_cli.ops.head import HeadOps
from daggerml_cli.ops.index import IndexOps
from daggerml_cli.ops.node import NodeOps
from daggerml_cli.types import Argspec, ArgvNode, Datum, KwargvNode


class TestSetupIndexParser:
    """Test index parser setup."""

    def test_parser_creation(self):
        """Test that parser is created with subcommands."""
        parser = ArgumentParser()
        setup_index_parser(parser)
        args = parser.parse_args(["list"])
        assert args.method == "list"
        args = parser.parse_args(["start-fn", "index:abc", "node:fn"])
        assert args.method == "start-fn"
        args = parser.parse_args(["delete", "index:abc"])
        assert args.method == "delete"
        args = parser.parse_args(["create", "--head", "head:main"])
        assert args.method == "create"
        args = parser.parse_args(["get-kwargv", "index:abc"])
        assert args.method == "get-kwargv"
        args = parser.parse_args(["get-argv", "index:abc"])
        assert args.method == "get-argv"
        args = parser.parse_args(["put-import", "index:abc", "dag:xyz"])
        assert args.method == "put-import"
        args = parser.parse_args(["put-literal", "index:abc", "true"])
        assert args.method == "put-literal"
        args = parser.parse_args(["commit", "index:abc", "node:result"])
        assert args.method == "commit"
        args = parser.parse_args(["dump", "commit:abc"])
        assert args.method == "dump"

    def test_start_fn_parser_args(self):
        """Test start-fn parser arguments."""
        parser = ArgumentParser()
        setup_index_parser(parser)
        args = parser.parse_args(
            [
                "start-fn",
                "index:abc",
                "node:fn",
                "node:arg1",
                "--kwargv",
                '{"x":"node:arg2"}',
                "--name",
                "result",
                "--no-cache",
            ]
        )
        assert args.index_ref == "index:abc"
        assert args.argv == ["node:fn", "node:arg1"]
        assert args.kwargv == '{"x":"node:arg2"}'
        assert args.name == "result"
        assert args.cache is False

    def test_create_parser_args(self):
        """Test create parser arguments."""
        parser = ArgumentParser()
        setup_index_parser(parser)
        args = parser.parse_args(["create", "--head", "head:main"])
        assert args.head == "head:main"
        assert args.dump is None
        args = parser.parse_args(["create", "--dump", "payload"])
        assert args.dump == "payload"
        assert args.head is None


class TestIndexParseHelpers:
    """Test JSON and ref parsing helpers."""

    def test_parse_json_value_primitives(self):
        """Parse JSON values for primitives and collections."""
        assert _parse_json_value("true") is True
        assert _parse_json_value("false") is False
        assert _parse_json_value("null") is None
        assert _parse_json_value("42") == 42
        assert _parse_json_value("3.5") == 3.5
        assert _parse_json_value("[1, 2]") == [1, 2]
        assert _parse_json_value('{"a": 1}') == {"a": 1}

    def test_parse_json_value_raw_string(self):
        """Return raw string when not JSON."""
        assert _parse_json_value("hello") == "hello"

    def test_parse_json_value_invalid_json(self):
        """Raise ValueError for invalid JSON-like strings."""
        with pytest.raises(ValueError, match="Invalid JSON value"):
            _parse_json_value("{bad}")

    def test_parse_ref_list(self):
        """Parse list of refs."""
        refs = _parse_ref_list(["node:abc", "node:def"])
        assert refs == [Ref("node:abc"), Ref("node:def")]

    def test_parse_kwargv(self):
        """Parse kwargv JSON into refs."""
        result = _parse_kwargv('{"x":"node:abc"}')
        assert result == {"x": Ref("node:abc")}

    def test_parse_kwargv_invalid_json(self):
        """Raise ValueError for invalid JSON."""
        with pytest.raises(ValueError, match="Invalid JSON for --kwargv"):
            _parse_kwargv("{bad}")

    def test_parse_kwargv_not_object(self):
        """Raise ValueError when kwargv is not object."""
        with pytest.raises(ValueError, match="--kwargv must be a JSON object"):
            _parse_kwargv("[1]")


class TestExecuteIndexHandlers:
    """Test index handler functions."""

    def test_execute_index_list(self):
        """Test execute_index_list handler."""
        mock_ops = Mock()
        mock_ops.list.return_value = [Ref("index:abc"), Ref("index:def")]

        result = execute_index_list(mock_ops, Namespace())

        mock_ops.list.assert_called_once_with()
        assert result == ["index:abc", "index:def"]

    def test_execute_index_start_fn(self):
        """Test execute_index_start_fn handler."""
        mock_ops = Mock()
        mock_ops.start_fn.return_value = Ref("node:out")

        args = Namespace(
            index_ref="index:abc",
            argv=["node:fn", "node:arg"],
            kwargv='{"x":"node:arg"}',
            name="result",
            cache=False,
        )
        result = execute_index_start_fn(mock_ops, args)

        mock_ops.start_fn.assert_called_once_with(
            Ref("index:abc"),
            [Ref("node:fn"), Ref("node:arg")],
            {"x": Ref("node:arg")},
            "result",
            False,
        )
        assert result == "node:out"

    def test_execute_index_delete(self):
        """Test execute_index_delete handler."""
        mock_ops = Mock()

        args = Namespace(index_ref="index:abc")
        result = execute_index_delete(mock_ops, args)

        mock_ops.delete.assert_called_once_with(Ref("index:abc"))
        assert result is None

    def test_execute_index_create(self):
        """Test execute_index_create handler."""
        mock_ops = Mock()
        mock_ops.create.return_value = Ref("index:abc")

        args = Namespace(head="head:main", dump=None)
        result = execute_index_create(mock_ops, args)

        mock_ops.create.assert_called_once_with(head=Ref("head:main"), dump=None)
        assert result == "index:abc"

    def test_execute_index_get_kwargv(self):
        """Test execute_index_get_kwargv handler."""
        mock_ops = Mock()
        mock_ops.get_kwargv.return_value = Ref("node:kwargv")

        args = Namespace(index_ref="index:abc")
        result = execute_index_get_kwargv(mock_ops, args)

        mock_ops.get_kwargv.assert_called_once_with(Ref("index:abc"))
        assert result == "node:kwargv"

    def test_execute_index_get_argv(self):
        """Test execute_index_get_argv handler."""
        mock_ops = Mock()
        mock_ops.get_argv.return_value = Ref("node:argv")

        args = Namespace(index_ref="index:abc")
        result = execute_index_get_argv(mock_ops, args)

        mock_ops.get_argv.assert_called_once_with(Ref("index:abc"))
        assert result == "node:argv"

    def test_execute_index_put_import(self):
        """Test execute_index_put_import handler."""
        mock_ops = Mock()
        mock_ops.put_import.return_value = Ref("node:imported")

        args = Namespace(index_ref="index:abc", dag_ref="dag:xyz", node_ref="node:123", name="n")
        result = execute_index_put_import(mock_ops, args)

        mock_ops.put_import.assert_called_once_with(Ref("index:abc"), Ref("dag:xyz"), Ref("node:123"), "n")
        assert result == "node:imported"

    def test_execute_index_put_literal(self):
        """Test execute_index_put_literal handler."""
        mock_ops = Mock()
        mock_ops.put_literal.return_value = Ref("node:lit")

        args = Namespace(index_ref="index:abc", value="true", name="literal")
        result = execute_index_put_literal(mock_ops, args)

        mock_ops.put_literal.assert_called_once_with(Ref("index:abc"), True, name="literal")
        assert result == "node:lit"

    def test_execute_index_commit(self):
        """Test execute_index_commit handler."""
        mock_ops = Mock()
        mock_ops.commit.return_value = Ref("commit:abc")

        args = Namespace(
            index_ref="index:abc",
            value_ref="node:result",
            head="head:main",
            message="done",
            dag_name="output",
        )
        result = execute_index_commit(mock_ops, args)

        mock_ops.commit.assert_called_once_with(
            Ref("index:abc"),
            Ref("node:result"),
            head=Ref("head:main"),
            message="done",
            dag_name="output",
        )
        assert result == "commit:abc"

    def test_execute_index_dump(self):
        """Test execute_index_dump handler."""
        mock_ops = Mock()
        mock_ops.dump.return_value = "payload"

        args = Namespace(commit_ref="commit:abc")
        result = execute_index_dump(mock_ops, args)

        mock_ops.dump.assert_called_once_with(Ref("commit:abc"))
        assert result == "payload"


class TestIndexCLIIntegration:
    """Integration tests for index CLI commands."""

    def setup_method(self):
        """Set up temporary repository for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.repo_path = self.temp_dir
        self.dml_ops = DmlOps.open(self.repo_path)
        self.base_ops = BaseOps(self.dml_ops._db)
        self.head_ops = HeadOps(_db=self.dml_ops._db)
        self.index_ops = IndexOps(_db=self.dml_ops._db)
        self.node_ops = NodeOps(_db=self.dml_ops._db)
        self.head_ref = self.head_ops.create("main")

    def teardown_method(self):
        """Clean up temporary repository."""
        import shutil

        shutil.rmtree(self.temp_dir)

    def run_cli_command(self, args):
        """Helper to run CLI command and capture output."""
        import sys
        from io import StringIO

        from daggerml_cli._cli import cli

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr

        sys.argv = ["dml", "--repo", self.repo_path] + args
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

    def test_index_create_with_head_and_dump(self):
        """Test index create works for head and dump inputs."""
        stdout, stderr = self.run_cli_command(["index", "create", "--head", self.head_ref.to])
        assert not stderr
        result = json.loads(stdout.strip())
        assert isinstance(result, str)
        assert result.startswith("index:")

        with self.base_ops._tx(readonly=False) as txn:
            argv_datum_ref = txn.put(Datum(data=[]))
            argv_node_ref = txn.put(ArgvNode(value=argv_datum_ref))
            kwargv_datum_ref = txn.put(Datum(data={}))
            kwargv_node_ref = txn.put(KwargvNode(value=kwargv_datum_ref))
            argspec_ref = txn.put(Argspec(argv=argv_node_ref, kwargv=kwargv_node_ref))
            dump_payload = txn.dump(argspec_ref)

        stdout, stderr = self.run_cli_command(["index", "create", "--dump", dump_payload])
        assert not stderr
        result = json.loads(stdout.strip())
        assert isinstance(result, str)
        assert result.startswith("index:")

    def test_index_put_literal_parses_json_and_strings(self):
        """Test put-literal parses JSON and raw strings."""
        stdout, stderr = self.run_cli_command(["index", "create", "--head", self.head_ref.to])
        assert not stderr
        index_ref = json.loads(stdout.strip())

        cases = [
            ('{"k": true}', {"k": True}),
            ("[1, 2]", [1, 2]),
            ("false", False),
            ("hello", "hello"),
        ]
        for raw_value, expected in cases:
            stdout, stderr = self.run_cli_command(["index", "put-literal", index_ref, raw_value])
            assert not stderr
            node_ref = json.loads(stdout.strip())
            assert self.node_ops.unroll(Ref(node_ref)) == expected

    def test_index_start_fn_no_cache(self):
        """Test start-fn supports mixed args and no-cache flag."""
        index_ref = self.index_ops.create(head=self.head_ref)
        fn_node = self.index_ops.put_literal(index_ref, Resource("daggerml:list"))
        arg_one = self.index_ops.put_literal(index_ref, 1)
        arg_two = self.index_ops.put_literal(index_ref, 2)

        stdout, stderr = self.run_cli_command(
            [
                "index",
                "start-fn",
                index_ref.to,
                fn_node.to,
                arg_one.to,
                arg_two.to,
                "--name",
                "result",
                "--no-cache",
            ]
        )
        assert not stderr
        node_ref = json.loads(stdout.strip())
        assert self.node_ops.unroll(Ref(node_ref)) == [1, 2]

    def test_index_commit_updates_head(self):
        """Test commit updates head and returns commit ref."""
        stdout, stderr = self.run_cli_command(["index", "create", "--head", self.head_ref.to])
        assert not stderr
        index_ref = json.loads(stdout.strip())

        stdout, stderr = self.run_cli_command(["index", "put-literal", index_ref, "42"])
        assert not stderr
        value_ref = json.loads(stdout.strip())

        stdout, stderr = self.run_cli_command(
            [
                "index",
                "commit",
                index_ref,
                value_ref,
                "--head",
                self.head_ref.to,
                "--message",
                "done",
                "--dag-name",
                "output",
            ]
        )
        assert not stderr
        commit_ref = json.loads(stdout.strip())
        assert commit_ref.startswith("commit:")

        with self.base_ops._tx(readonly=True) as txn:
            head_obj = txn.get(self.head_ref)
        assert head_obj.commit.to == commit_ref

    def test_invalid_json_returns_error(self):
        """Test invalid JSON arguments return JSON error output."""
        stdout, stderr = self.run_cli_command(["index", "create", "--head", self.head_ref.to])
        assert not stderr
        index_ref = json.loads(stdout.strip())

        stdout, stderr = self.run_cli_command(["index", "put-literal", index_ref, "{bad}"])
        assert stderr
        error_data = json.loads(stderr.strip())
        assert "error" in error_data
