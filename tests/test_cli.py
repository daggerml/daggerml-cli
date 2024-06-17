import os
import unittest
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from click.testing import CliRunner
from click.testing import Result as ClickResult

from daggerml_cli.cli import cli, from_json, to_json


@dataclass
class Api:
    _config_dir: str
    _project_dir: str

    def invoke(self, *args, raw=False):
        flags = ['--config-dir', self._config_dir, '--project-dir', self._project_dir]
        result = CliRunner().invoke(cli, [*flags, *args])
        if raw:
            return result
        return result.output

    @property
    def config_dir(self):
        return Path(self._config_dir)

    @property
    def project_dir(self):
        return Path(self._project_dir)


@contextmanager
def tmpdirs(*, init=True):
    with TemporaryDirectory() as tmpd0, TemporaryDirectory() as tmpd1:
        api = Api(tmpd0, tmpd1)
        if init:
            api.invoke('repo', 'create', 'foopy')
            api.invoke('project', 'init', 'foopy')
        yield api

class TestApiCreate(unittest.TestCase):

    def test_create_repo(self):
        with tmpdirs(init=False) as api:
            conf_dir, proj_dir = api.config_dir, api.project_dir
            result = api.invoke('repo', 'create', 'foopy', raw=True)
            assert isinstance(result, ClickResult)
            assert result.output == "Created repository: foopy\n"
            assert result.exit_code == 0
            assert os.path.isdir(conf_dir)
            assert os.path.isdir(proj_dir)
            assert len(os.listdir(conf_dir)) > 0
            assert len(os.listdir(proj_dir)) == 0
            result = api.invoke('project', 'init', 'foopy', raw=True)
            assert isinstance(result, ClickResult)
            assert result.output == "Initialized project with repo: foopy\n"
            assert result.exit_code == 0
            assert len(os.listdir(proj_dir)) > 0
        assert not os.path.isdir(conf_dir)
        assert not os.path.isdir(proj_dir)

    def test_create_dag(self):
        with tmpdirs() as api:
            repo = api.invoke(
                'dag', 'create', 'cool-name', 'doopy',
            )
            assert isinstance(repo, str)
            node = api.invoke(
                'dag', 'invoke', repo,
                to_json(['put_literal', [], {'data': {'asdf': 23}}])
            )
            api.invoke(
                'dag', 'invoke', repo,
                to_json(['commit', [], {'result': from_json(node)}])
            )
            api.invoke('dag', 'get', 'cool-name')

    def test_repo_dump(self):
        with tmpdirs() as to_api:
            with tmpdirs() as from_api:
                repo = from_api.invoke(
                    'dag', 'create', 'cool-name', 'doopy',
                )
                node = from_api.invoke(
                    'dag', 'invoke', repo,
                    to_json(['put_literal', [], {'data': {'asdf': 23}}])
                )
