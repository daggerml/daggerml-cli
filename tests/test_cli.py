import json
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from typing import Any
from unittest import TestCase

import click
import pytest
from click.testing import CliRunner

from daggerml_cli.cli import cli, from_json, jsdumps, to_json
from daggerml_cli.repo import Resource

SUM = Resource("./tests/fn/sum.py", adapter="dml-python-fork-adapter")


@dataclass
class Dag:
    _dml: Any
    _token: str

    def __call__(self, op, *args, _flags=None, **kwargs):
        _flags = _flags or []
        return self._dml(*_flags, "api", "invoke", self._token, input=to_json([op, args, kwargs]))

    def json(self, op, *args, **kwargs):
        return self(op, *args, **kwargs)


@dataclass
class Cli:
    """Helper class facilitating testing of cli `dag invoke` command."""

    _config_dir: str
    _project_dir: str
    _cache_path: str

    def _flags(self):
        out = [
            "--project-dir",
            self._project_dir,
            "--config-dir",
            self._config_dir,
        ]
        if isinstance(self._cache_path, str):
            out += ["--cache-path", self._cache_path]
        return out

    def __call__(self, *args, input=None):
        args = [*self._flags(), *args]
        kw = {}
        if tuple(map(int, click.__version__.split(".")[:2])) < (8, 2):
            kw["mix_stderr"] = False
        resp = CliRunner(**kw).invoke(cli, args, catch_exceptions=False, input=input)
        print(resp.stderr, file=sys.stderr)
        return resp.output.rstrip()

    def json(self, *args):
        return json.loads(self(*args))

    def branch(self, expected):
        assert json.loads(self("status"))["branch"] == expected

    def branch_create(self, name):
        assert self("branch", "create", name) == f"Created branch: {name}"

    def branch_delete(self, name):
        assert self("branch", "delete", name) == f"Deleted branch: {name}"

    def branch_list(self, *expected):
        assert self("branch", "list") == jsdumps(expected)

    def config_branch(self, name):
        assert self("config", "branch", name) == f"Selected branch: {name}"

    def config_repo(self, name):
        assert self("config", "repo", name) == f"Selected repository: {name}"

    def config_user(self, name):
        assert self("config", "user", name) == f"Set user: {name}"

    def dag_create(self, name, message, dump=None):
        return Dag(self, self("api", "create", name, message, input=dump))

    def repo(self, expected):
        assert json.loads(self("status"))["repo"] == expected
        assert self("--query", "[?current==`true`].name|[0]", "repo", "list") == jsdumps(expected)

    def repo_copy(self, to):
        current_repo = json.loads(self("status"))["repo"]
        assert self("repo", "copy", to) == f"Copied repository: {current_repo} -> {to}"

    def repo_create(self, name):
        assert self("repo", "create", name) == f"Created repository: {name}"

    def repo_delete(self, name):
        assert self("repo", "delete", name) == f"Deleted repository: {name}"

    def repo_gc(self, expected):
        assert self("repo", "gc") == expected

    def repo_list(self, *expected):
        assert self("--query", "[*].name", "repo", "list") == jsdumps(expected)


@contextmanager
def cliTmpDirs(config_dir=None, project_dir=None, cache_path=None):
    tmpdirs = [TemporaryDirectory(prefix="dml-test-") for _ in range(3)]
    try:
        yield Cli(
            config_dir or tmpdirs[0].name,
            project_dir or tmpdirs[1].name,
            cache_path or tmpdirs[2].name,
        )
    finally:
        for tmpd in tmpdirs:
            tmpd.cleanup()


class TestCliBranch(TestCase):
    def test_branch_create(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            dml.branch_create("b0")

    def test_branch_delete(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            dml.branch_create("b0")
            dml.config_branch("main")
            dml.branch_list("b0", "main")
            dml.branch_delete("b0")
            dml.branch_list("main")

    def test_branch_list(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            dml.branch_create("b0")
            dml.branch_create("b1")
            dml.branch_create("b2")
            dml.branch_list("b0", "b1", "b2", "main")

    @pytest.mark.skip(reason="TODO: write test")
    def test_branch_merge(self):
        pass

    @pytest.mark.skip(reason="TODO: write test")
    def test_branch_rebase(self):
        pass

    def test_branch_use(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            dml.branch_create("b0")
            dml.branch("b0")
            dml.config_branch("main")
            dml.branch("main")


class TestCliCommit(TestCase):
    def test_commit_list(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            commits = json.loads(dml("commit", "list"))
            assert len(commits) == 1


class TestCliDag(TestCase):
    def test_dag_describe(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            d0 = dml.dag_create("d0", "dag d0")
            v0 = Resource("a:b/asdf:e")
            d0("commit", result=from_json(d0("put_literal", data=v0, name="qwer")))
            desc = dml.json("dag", "describe", "d0")
            assert len(desc["nodes"]) == 1
            assert desc["nodes"][0]["name"] == "qwer"
            desc2 = dml.json("dag", "describe", desc["id"])
            assert desc == desc2

    def test_dag_list(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            assert dml.json("dag", "list") == []
            assert dml.json("dag", "list", "--all") == []
            d0 = dml.dag_create("d0", "dag d0")
            assert dml.json("dag", "list", "--all") == []
            v0 = d0("put_literal", data=SUM, name="sum-fn")
            ns = [d0("put_literal", data=i, name=f"a{i}") for i in range(3)]
            r0 = d0("start_fn", argv=list(map(from_json, [v0, *ns])))
            d0("commit", result=from_json(r0))
            daglist = dml.json("dag", "list")
            assert len(daglist) == 1
            assert list(daglist[0]) == [
                "id",
                "error",
                "name",
                "names",
                "nodes",
                "result",
            ]
            assert daglist[0]["error"] is None
            assert list(daglist[0]["names"]) == [
                *[f"a{i}" for i in range(3)],
                "sum-fn",
            ]
            desc = dml.json("dag", "describe", daglist[0]["id"])
            assert desc["id"] == daglist[0]["id"]
            daglist = dml.json("dag", "list", "--all")
            assert len(daglist) >= 2


class TestCliNode:
    @pytest.mark.parametrize(
        "obj,expected",
        [
            ({"a": 1, "b": 2}, {"node_type": "literal", "data_type": "dict", "length": 2, "keys": ["a", "b"]}),
            ([1, 2, 3], {"node_type": "literal", "data_type": "list", "length": 3, "keys": None}),
            (1, {"node_type": "literal", "data_type": "int", "length": None, "keys": None}),
            ("hello", {"node_type": "literal", "data_type": "str", "length": None, "keys": None}),
        ],
    )
    def test_node_describe(self, obj, expected):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            d0 = dml.dag_create("d0", "dag d0")
            v0 = d0("put_literal", data=obj)
            node_info = dml.json("node", "describe", from_json(v0).to)
            assert {k: v for k, v in node_info.items() if k in expected} == expected
            assert node_info["datum_id"].startswith("datum/")


class TestCliProject(TestCase):
    def test_project_init(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")


class TestCliRepo(TestCase):
    def test_repo_copy(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.repo_create("repo1")
            with self.assertRaises(AssertionError):
                dml.repo_copy("repo2")
            dml.config_repo("repo0")
            dml.repo_copy("repo2")
            dml.repo_list("repo0", "repo1", "repo2")

    def test_repo_create(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.repo_create("repo1")

    def test_repo_delete(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.repo_create("repo1")
            dml.repo_list("repo0", "repo1")
            dml.repo_delete("repo1")
            dml.repo_list("repo0")
            dml.repo_delete("repo0")
            dml.repo_list()

    def test_repo_gc(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            dml.branch_create("b0")
            d0 = dml.dag_create("d0", "dag d0")
            v0 = Resource("a:b/asdf:e")
            d0("commit", result=from_json(d0("put_literal", data=v0)))
            dml.config_branch("main")
            dml.branch_delete("b0")
            resp = dml("repo", "gc")
            lines = [[y for y in x.split() if y] for x in resp.split("\n") if x]
            assert lines.pop(0) == ["object", "deleted", "remaining"]
            assert all(not x[0].isnumeric() for x in lines)
            assert all(x[1].isnumeric() for x in lines)
            assert all(x[2].isnumeric() for x in lines)
            resp = dml("repo", "gc")
            lines = [[y for y in x.split() if y] for x in resp.split("\n") if x]
            assert all(x[1].strip() == "0" for x in lines[1:])

    def test_repo_list(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_list()
            dml.repo_create("repo0")
            dml.repo_list("repo0")
            dml.repo_create("repo1")
            dml.repo_list("repo0", "repo1")


class TestCliStatus(TestCase):
    def test_status_unset(self):
        with cliTmpDirs(cache_path=True) as dml:
            status = json.loads(dml("status"))
            assert status == {
                "repo": None,
                "branch": None,
                "user": None,
                "config_dir": dml._config_dir,
                "project_dir": dml._project_dir,
                "cache_path": os.path.expanduser("~/.config/dml/cachedb"),
            }

    def test_status_set(self):
        with cliTmpDirs() as dml:
            dml.config_user("Testy McTesterstein")
            dml.repo_create("repo0")
            dml.config_repo("repo0")
            dml.branch_create("b0")
            status = json.loads(dml("status"))
            assert status == {
                "repo": "repo0",
                "branch": "b0",
                "user": "Testy McTesterstein",
                "config_dir": dml._config_dir,
                "project_dir": dml._project_dir,
                "cache_path": dml._cache_path,
            }
