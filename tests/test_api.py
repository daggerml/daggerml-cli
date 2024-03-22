import unittest
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Error, Node, Repo, Resource, unroll_datum


def dump(repo, count=None):
    rows = []
    for db in repo.dbs.keys():
        [rows.append([len(rows) + 1, k.to, k()]) for k in repo.cursor(db)]
    rows = rows[:min(count, len(rows))] if count is not None else rows
    print('\n' + tabulate(rows, tablefmt="simple_grid"))

class TestApiCreate(unittest.TestCase):

    def test_create_dag(self):
        with TemporaryDirectory() as tmpd0, TemporaryDirectory() as tmpd1:
            ctx = Config(
                _CONFIG_DIR=tmpd0,
                _PROJECT_DIR=tmpd1,
                _USER='user0',
            )
            api.create_repo(ctx, 'test')
            assert api.list_repo(ctx) == ['test']
            api.use_repo(ctx, 'test')
            assert api.list_branch(ctx) == ['main']
            api.init_project(ctx, 'test')
            api.create_branch(ctx, 'b0')
            assert api.current_branch(ctx) == 'b0'


class TestApiBase(unittest.TestCase):

    def setUp(self):
        self.tmpdir_ctx = [TemporaryDirectory(), TemporaryDirectory()]
        self.tmpdirs = [x.__enter__() for x in self.tmpdir_ctx]
        self.CTX = ctx = Config(
            _CONFIG_DIR=self.tmpdirs[0],
            _PROJECT_DIR=self.tmpdirs[1],
            _USER='user0',
        )
        api.create_repo(ctx, 'test')
        api.use_repo(ctx, 'test')
        api.init_project(ctx, 'test')

    def tearDown(self):
        for x in self.tmpdir_ctx:
            x.__exit__(None, None, None)

    def wrap(self, tok):
        def inner(op, *args, **kwargs):
            return api.invoke_api(self.CTX, tok, [op, args, kwargs])
        inner.tok = tok
        return inner

    def begin(self, name, message):
        tok, dag = api.begin_dag(self.CTX, name, message)
        return self.wrap(tok), dag

    @contextmanager
    def tx(self, write=False):
        db = Repo(self.CTX.REPO_PATH, self.CTX.USER, self.CTX.BRANCHREF)
        with db.tx(write):
            yield db

    def test_create_dag(self):
        # dag 0
        d0, dag = self.begin('d0', 'dag 0')
        data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
        n0 = d0('put_literal', dag, data)
        d0('commit', dag, n0)
        # dag 1
        d1, dag = self.begin('d1', 'dag 1')
        n0 = d1('put_load', dag, 'd0')
        with self.tx():
            assert isinstance(n0(), Node)
            val = n0().value
        n1 = d1('put_literal', dag, [val, val, 2])
        with self.tx():
            assert unroll_datum(n1().value) == [data, data, 2]
        d1('commit', dag, n1)

    def test_fn(self):
        rsrc = Resource('asdf')
        d0, dag = self.begin('d0', 'dag 0')
        n0 = d0('put_literal', dag, rsrc)
        n1 = d0('put_literal', dag, 1)
        with self.tx():
            assert isinstance(n0(), Node)
            assert isinstance(n1(), Node)
        fn = d0('start_fn', expr=[n0, n1], dag=dag)
        expr = d0('get_expr', fn)
        assert expr == [rsrc, 1]
        n2 = d0('put_literal', fn, {'asdf': 128})
        n3 = d0('commit', fn, n2, parent_dag=dag)
        d0('commit', dag, n3)
        resp = api.invoke_api(self.CTX, None, ['get_node_value', [n2], {}])
        assert resp == {'asdf': 128}

    def test_fn_meta(self):
        d0, dag = self.begin('d0', 'dag 0')
        n0 = d0('put_literal', dag, Resource('asdf'))
        n1 = d0('put_literal', dag, 1)
        fn = d0('start_fn', dag, [n0, n1])
        assert d0('get_fn_meta', fn) == ''
        assert d0('update_fn_meta', fn, '', 'asdfqwer') is None
        assert d0('get_fn_meta', fn) == 'asdfqwer'
        with self.assertRaisesRegex(Error, 'old metadata'):
            assert d0('update_fn_meta', fn, '', 'asdfqwer') is None
        assert d0('get_fn_meta', fn) == 'asdfqwer'
        n2 = d0('put_literal', fn, {'asdf': 128})
        d0('commit', fn, n2, parent_dag=dag)
        with self.tx():
            assert fn().meta == ''
            assert list(fn().result().value().value.keys()) == ['asdf']
