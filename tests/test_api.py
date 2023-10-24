import unittest
from functools import wraps
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Error, Node, Fn, Repo, Resource, unroll_datum


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

    def begin(self, name, message, ctx=None):
        @wraps(api.invoke_api)
        def inner(op, *args, **kwargs):
            return api.invoke_api(ctx, tok, [op, args, kwargs])
        ctx = self.CTX if ctx is None else ctx
        tok = api.invoke_api(ctx, None, ['begin', [name, message], {}])
        inner.tx = Repo.from_state(tok).tx
        return inner

    def test_create_dag(self):

        # dag 0
        d0 = self.begin('d0', 'dag 0')
        data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
        res = d0('put_literal', data)
        res = d0('commit', res)

        # dag 1
        d1 = self.begin('d1', 'dag 1')
        res = d1('put_load', 'd0')
        with d1.tx():
            assert isinstance(res(), Node)
            val = res().value()
        ref = d1('put_literal', [val, val, 2])
        res = d1('get_node', ref)
        with d1.tx():
            assert unroll_datum(res.value) == [data, data, 2]
        res = d1('commit', ref)

    def test_fn(self):
        d0 = self.begin('d0', 'dag 0')
        n0 = d0('put_literal', Resource({'asdf', 2}))
        n1 = d0('put_literal', 1)
        with d0.tx():
            assert isinstance(n0(), Node)
            assert isinstance(n1(), Node)
        expr = [n0, n1]
        n2 = d0('put_fn', expr, {'foo': 1})
        found = None
        try:
            n2 = d0('put_fn', expr, {'foo': 2})
            with d0.tx():
                assert isinstance(n2, Fn)
        except Error as e:
            with d0.tx():
                found = e.context['found']
                assert found.info == {'foo': 1}
                assert (found.value or found.error) is None
        assert found is not None
        n2 = d0('put_fn', expr, {'foo': 2}, replacing=found)
        with d0.tx():
            assert isinstance(n2, Fn)
            assert n2.info == {'foo': 2}
            assert (n2.value or n2.error) is None
        n3 = d0('put_fn', expr, None, {'foo': 2}, replacing=n2)
        n = d0('get_node', n3)
        with d0.tx():
            assert isinstance(n, Node)
            assert n.node.info is None
            assert unroll_datum(n.value) == {'foo': 2}
            assert n.error is None
        d0('commit', n3)

    def test_fn_w_error(self):
        d0 = self.begin('d0', 'dag 0')
        n0 = d0('put_literal', Resource({'asdf', 2}))
        n1 = d0('put_literal', 1)
        expr = [n0, n1]
        error = Error('fooby', {'asdf': 23})
        n2 = d0('put_fn', expr, None, None, error)
        n1 = d0('get_node', n2)
        with d0.tx():
            assert unroll_datum(n1.value) is None
            assert n1.error == error
            assert n1.error.code == 'Error'
            assert n1.error.message == 'fooby'
            assert n1.error.context == {'asdf': 23}

    def test_fn_w_value(self):
        d0 = self.begin('d0', 'dag 0')
        n0 = d0('put_literal', Resource({'asdf', 2}))
        n1 = d0('put_literal', 1)
        expr = [n0, n1]
        value = {'asdf': 23}
        n2 = d0('put_fn', expr, None, value)
        n1 = d0('get_node', n2)
        with d0.tx():
            assert isinstance(unroll_datum(n1.value), dict)
            assert unroll_datum(n1.value) == value
