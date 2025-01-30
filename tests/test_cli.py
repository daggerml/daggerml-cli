import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from typing import Any
from unittest import TestCase, mock

import pytest
from click.testing import CliRunner

from daggerml_cli.cli import cli, from_json, jsdumps, to_json
from daggerml_cli.repo import Resource


@dataclass
class Dag:
    _dml: Any
    _token: str

    def __call__(self, op, *args, **kwargs):
        if op == 'commit':
            idx, = self._dml.json('index', 'list')
        return self._dml('api', 'invoke', self._token, to_json([op, args, kwargs]))

    def json(self, op, *args, **kwargs):
        with mock.patch.dict(os.environ, {'DML_OUTPUT', 'json'}):
            return self(op, *args, **kwargs)


@dataclass
class Cli:
    """Helper class facilitating testing of cli `dag invoke` command."""
    _config_dir: str
    _project_dir: str

    def __call__(self, *args):
        args = ['--project-dir', self._project_dir, '--config-dir', self._config_dir, *args]
        return CliRunner().invoke(cli, args, catch_exceptions=False).output.rstrip()

    def json(self, *args):
        return json.loads(self(*args))

    def branch(self, expected):
        assert json.loads(self('status'))['branch'] == expected

    def branch_create(self, name):
        assert self('branch', 'create', name) == f'Created branch: {name}'

    def branch_delete(self, name):
        assert self('branch', 'delete', name) == f'Deleted branch: {name}'

    def branch_list(self, *expected):
        assert self('branch', 'list') == jsdumps(expected)

    def config_branch(self, name):
        assert self('config', 'branch', name) == f'Selected branch: {name}'

    def config_repo(self, name):
        assert self('config', 'repo', name) == f'Selected repository: {name}'

    def config_user(self, name):
        assert self('config', 'user', name) == f'Set user: {name}'

    def dag_create(self, name, message):
        return Dag(self, self('api', 'create', name, message))

    def repo(self, expected):
        assert json.loads(self('status'))['repo'] == expected
        assert self('--query', '[?current==`true`].name|[0]', 'repo', 'list') == jsdumps(expected)

    def repo_copy(self, to):
        current_repo = json.loads(self('status'))['repo']
        assert self('repo', 'copy', to) == f'Copied repository: {current_repo} -> {to}'

    def repo_create(self, name):
        assert self('repo', 'create', name) == f'Created repository: {name}'

    def repo_delete(self, name):
        assert self('repo', 'delete', name) == f'Deleted repository: {name}'

    def repo_gc(self, expected):
        assert self('repo', 'gc') == expected

    def repo_list(self, *expected):
        assert self('--query', '[*].name', 'repo', 'list') == jsdumps(expected)


@contextmanager
def cliTmpDirs():
    with TemporaryDirectory(prefix='dml-test-') as config_dir:
        with TemporaryDirectory(prefix='dml-test-') as project_dir:
            yield Cli(config_dir, project_dir)


class TestCliBranch(TestCase):

    def test_branch_create(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.config_repo('repo0')
            dml.branch_create('b0')

    def test_branch_delete(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.config_repo('repo0')
            dml.branch_create('b0')
            dml.config_branch('main')
            dml.branch_list('b0', 'main')
            dml.branch_delete('b0')
            dml.branch_list('main')

    def test_branch_list(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.config_repo('repo0')
            dml.branch_create('b0')
            dml.branch_create('b1')
            dml.branch_create('b2')
            dml.branch_list('b0', 'b1', 'b2', 'main')

    @pytest.mark.skip(reason="TODO: write test")
    def test_branch_merge(self):
        pass

    @pytest.mark.skip(reason="TODO: write test")
    def test_branch_rebase(self):
        pass

    def test_branch_use(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.config_repo('repo0')
            dml.branch_create('b0')
            dml.branch('b0')
            dml.config_branch('main')
            dml.branch('main')


class TestCliCommit(TestCase):

    def test_commit_list(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.config_repo('repo0')
            commits = json.loads(dml('commit', 'list'))
            assert len(commits) == 1


class TestCliProject(TestCase):

    def test_project_init(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.config_repo('repo0')
            # dml.repo('repo0')


class TestCliRepo(TestCase):

    def test_repo_copy(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.repo_create('repo1')
            with self.assertRaises(AssertionError):
                dml.repo_copy('repo2')
            dml.config_repo('repo0')
            dml.repo_copy('repo2')
            dml.repo_list('repo0', 'repo1', 'repo2')

    def test_repo_create(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.repo_create('repo1')

    def test_repo_delete(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.repo_create('repo1')
            dml.repo_list('repo0', 'repo1')
            dml.repo_delete('repo1')
            dml.repo_list('repo0')
            dml.repo_delete('repo0')
            dml.repo_list()

    def test_repo_gc(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_create('repo0')
            dml.config_repo('repo0')
            dml.branch_create('b0')
            d0 = dml.dag_create('d0', 'dag d0')
            v0 = Resource('a:b/asdf:e')
            d0('commit', result=from_json(d0('put_literal', data=v0)))
            dml.config_branch('main')
            dml.branch_delete('b0')
            dml.repo_gc(v0.uri)

    def test_repo_list(self):
        with cliTmpDirs() as dml:
            dml.config_user('Testy McTesterstein')
            dml.repo_list()
            dml.repo_create('repo0')
            dml.repo_list('repo0')
            dml.repo_create('repo1')
            dml.repo_list('repo0', 'repo1')
