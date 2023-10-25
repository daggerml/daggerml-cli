import unittest
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Node, Resource, unroll_datum


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
        inner.tx = tok.tx
        inner.begin = lambda *expr: self.wrap(inner('begin', expr=expr))
        return inner

    def begin(self, name, message):
        return self.wrap(api.invoke_api(self.CTX, None, ['begin', [], dict(name=name, message=message)]))

    def test_create_dag(self):

        # dag 0
        d0 = self.begin('d0', 'dag 0')
        data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
        n0 = d0('put_literal', data)
        d0('commit', n0)

        # dag 1
        d1 = self.begin('d1', 'dag 1')
        n0 = d1('put_load', 'd0')
        with d1.tx():
            assert isinstance(n0(), Node)
            val = n0().value()
        n1 = d1('put_literal', [val, val, 2])
        with d1.tx():
            assert unroll_datum(n1().value) == [data, data, 2]
        d1('commit', n1)

    def test_fn(self):
        d0 = self.begin('d0', 'dag 0')
        n0 = d0('put_literal', Resource({'asdf': 2}))
        n1 = d0('put_literal', 1)
        with d0.tx():
            assert isinstance(n0(), Node)
            assert isinstance(n1(), Node)
        fn = d0.begin(n0, n1)
        n2 = fn('put_literal', 128)
        n3 = fn('commit', n2)
        d0('commit', n3)

#   def test_fn_w_error(self):
#       d0 = self.begin('d0', 'dag 0')
#       n0 = d0('put_literal', Resource({'asdf', 2}))
#       n1 = d0('put_literal', 1)
#       expr = [n0, n1]
#       error = Error('fooby', {'asdf': 23})
#       n2 = d0('begin_fn', expr, None, None, error)
#       n1 = d0('get_node', n2)
#       with d0.tx():
#           assert unroll_datum(n1.value) is None
#           assert n1.error == error
#           assert n1.error.code == 'Error'
#           assert n1.error.message == 'fooby'
#           assert n1.error.context == {'asdf': 23}

#   def test_fn_w_value(self):
#       d0 = self.begin('d0', 'dag 0')
#       n0 = d0('put_literal', Resource({'asdf', 2}))
#       n1 = d0('put_literal', 1)
#       expr = [n0, n1]
#       value = {'asdf': 23}
#       n2 = d0('begin_fn', expr, None, value)
#       n1 = d0('get_node', n2)
#       with d0.tx():
#           assert isinstance(unroll_datum(n1.value), dict)
#           assert unroll_datum(n1.value) == value
