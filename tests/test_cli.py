import json
import os
import unittest
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from click.testing import CliRunner
from click.testing import Result as ClickResult

from daggerml_cli.cli import cli, from_json, to_json
from daggerml_cli.repo import Resource


@dataclass
class Api:
    _config_dir: str
    _project_dir: str

    def invoke(self, *args):
        flags = ['--config-dir', self._config_dir, '--project-dir', self._project_dir]
        result = CliRunner().invoke(cli, [*flags, *args])
        return result

    def __call__(self, *args, output='text'):
        result = self.invoke('--output', output, *args).output
        if output == 'text':
            return result
        return [json.loads(x) for x in result.split('\n') if len(x) > 0]

    @property
    def config_dir(self):
        return Path(self._config_dir)

    @property
    def project_dir(self):
        return Path(self._project_dir)

    def init(self, name='foopy'):
        self('repo', 'create', name)
        self('project', 'init', name)


@contextmanager
def tmpdirs(*, init=True):
    with TemporaryDirectory(prefix='dml-test-') as tmpd0, TemporaryDirectory(prefix='dml-test-') as tmpd1:
        api = Api(tmpd0, tmpd1)
        if init:
            api.init()
        yield api

class TestApiCreate(unittest.TestCase):

    def test_create_repo(self):
        with tmpdirs(init=False) as api:
            conf_dir, proj_dir = api.config_dir, api.project_dir
            result = api.invoke('repo', 'create', 'foopy')
            assert isinstance(result, ClickResult)
            assert result.output == "Created repository: foopy\n"
            assert result.exit_code == 0
            assert os.path.isdir(conf_dir)
            assert os.path.isdir(proj_dir)
            assert len(os.listdir(conf_dir)) > 0
            assert len(os.listdir(proj_dir)) == 0
            result = api.invoke('project', 'init', 'foopy')
            assert isinstance(result, ClickResult)
            assert result.output == "Initialized project with repo: foopy\n"
            assert result.exit_code == 0
            assert len(os.listdir(proj_dir)) > 0
        assert not os.path.isdir(conf_dir)
        assert not os.path.isdir(proj_dir)

    def test_create_dag(self):
        with tmpdirs() as api:
            repo = api(
                'branch', 'create', 'cool-branch'
            )
            repo = api(
                'branch', 'use', 'cool-branch'
            )
            repo = api(
                'dag', 'create', 'cool-name', 'doopy',
            )
            rsrc = Resource('a:b/asdf:e')
            api(
                'dag', 'invoke', repo,
                to_json(['put_literal', [], {'data': rsrc}])
            )
            node = api(
                'dag', 'invoke', repo,
                to_json(['put_literal', [], {'data': {'asdf': 23}}])
            )
            idx, = api('index', 'list', output='json')
            dag_result = api(
                'dag', 'invoke', repo,
                to_json(['commit', [], {'result': from_json(node)}])
            )
            assert from_json(dag_result).to == idx['dag']
            # tmp = [json.loads(x) for x in api('dag', 'list').split('\n') if len(x)]
            tmp = api('dag', 'list', output='json')
            assert [x['name'] for x in tmp] == ['cool-name']
            assert api('dag', 'list', '-n', 'asdf') == ''
            dag_id, = (x['id'] for x in tmp)
            tmp, = api('dag', 'describe', dag_id, output='json')
            assert sorted(tmp.keys()) == ['edges', 'error', 'expr', 'id', 'nodes', 'result']
            assert tmp['error'] is None
            assert isinstance(tmp['result'], str)
            assert [x['name'] for x in api('dag', 'list', output='json')] == ['cool-name']
            assert api('branch', 'list') == 'cool-branch\nmain\n'
            api('branch', 'use', 'main')
            assert api('dag', 'list') == ''
            api('branch', 'delete', 'cool-branch')
            assert api('branch', 'list') == 'main\n'
            resp = api('repo', 'gc')
            assert resp == f'{rsrc.uri}\n'
