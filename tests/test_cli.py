import os
import unittest
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from click.testing import CliRunner

from daggerml_cli.cli import cli, from_json, to_json


def invoke(*args, raw=False):
    result = CliRunner().invoke(cli, args)
    if raw:
        return result
    return result.output

def deref(ref):
    res = invoke('dag', 'invoke', to_json(None), to_json(['get_ref', [from_json(ref)], {}]))
    return from_json(res)


@contextmanager
def tmpdirs(*, init=True):
    with TemporaryDirectory() as tmpd0, TemporaryDirectory() as tmpd1:
        old_env = dict(os.environ)
        os.environ['DML_CONFIG_DIR'] = tmpd0
        os.environ['DML_PROJECT_DIR'] = tmpd1
        if init:
            invoke('repo', 'create', 'foopy')
            invoke('project', 'init', 'foopy')
        yield tmpd0, tmpd1
        os.environ.clear()
        os.environ.update(old_env)

class TestApiCreate(unittest.TestCase):

    def test_create_repo(self):
        with tmpdirs(init=False) as (conf_dir, proj_dir):
            conf_dir, proj_dir = Path(conf_dir), Path(proj_dir)
            result = invoke('repo', 'create', 'foopy', raw=True)
            assert result.output == "Created repository: foopy\n"
            assert result.exit_code == 0
            assert os.path.isdir(conf_dir)
            assert os.path.isdir(proj_dir)
            assert len(os.listdir(conf_dir)) > 0
            assert len(os.listdir(proj_dir)) == 0
            result = invoke('project', 'init', 'foopy', raw=True)
            assert result.output == "Initialized project with repo: foopy\n"
            assert result.exit_code == 0
            assert len(os.listdir(proj_dir)) > 0
        assert not os.path.isdir(conf_dir)
        assert not os.path.isdir(proj_dir)

    def test_create_dag(self):
        with tmpdirs():
            repo = invoke(
                'dag', 'create', 'cool-name', 'doopy',
            )
            assert isinstance(repo, str)
            node = invoke(
                'dag', 'invoke', repo,
                to_json(['put_literal', [], {'data': {'asdf': 23}}])
            )
            invoke(
                'dag', 'invoke', repo,
                to_json(['commit', [], {'result': from_json(node)}])
            )
            invoke('dag', 'get', 'cool-name')
