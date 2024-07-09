import unittest
from contextlib import contextmanager
from dataclasses import dataclass, field, fields, replace
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Error, FnWaiter, Node, Ref, Repo, Resource, unroll_datum


def dump(repo, count=None):
    rows = []
    for db in repo.dbs.keys():
        [rows.append([len(rows) + 1, k.to, k()]) for k in repo.cursor(db)]
    rows = rows[:min(count, len(rows))] if count is not None else rows
    print('\n' + tabulate(rows, tablefmt="simple_grid"))


@dataclass
class SimpleApi:
    token: Ref
    ctx: Config
    tmpdirs: list = field(default_factory=list)

    def __call__(self, op, *args, **kwargs):
        return api.invoke_api(self.ctx, self.token, [op, args, kwargs])

    @classmethod
    def begin(cls, name, message, ctx=None, dag_dump=None, dirlist=None):
        tmpdirs = []
        if ctx is None:
            tmpdirs = [TemporaryDirectory(), TemporaryDirectory()]
            ctx = Config(
                _CONFIG_DIR=tmpdirs[0].__enter__(),
                _PROJECT_DIR=tmpdirs[1].__enter__(),
                _USER='user0',
            )
            api.create_repo(ctx, 'test')
            api.use_repo(ctx, 'test')
            api.init_project(ctx, 'test')
        tok = api.begin_dag(ctx, name, message, dag_dump=dag_dump)
        return cls(tok, ctx, tmpdirs)

    @contextmanager
    def tx(self, write=False):
        db = Repo(self.ctx.REPO_PATH, self.ctx.USER, self.ctx.BRANCHREF)
        with db.tx(write):
            yield db

    def cleanup(self):
        for x in self.tmpdir_ctx:
            x.__exit__(None, None, None)

    def start_fn(self, expr, use_cache=False):
        waiter, dump = self('start_fn', expr, use_cache=use_cache)
        fnapi = SimpleApi.begin('dag', 'message', dag_dump=dump)
        return waiter, fnapi


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

    def test_create_dag(self):
        # dag 0
        d0 = SimpleApi.begin('d0', 'dag 0')
        data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
        n0 = d0('put_literal', data)
        d0('commit', n0)
        # dag 1
        d1 = SimpleApi.begin('d0', 'dag 0', ctx=d0.ctx)
        n0 = d1('put_load', 'd0')
        with d1.tx():
            assert isinstance(n0(), Node)
            val = n0().value
        n1 = d1('put_literal', [val, val, 2])
        with d1.tx():
            assert unroll_datum(n1().value) == [data, data, 2]
        d1('commit', n1)

    def test_fn(self):
        d0 = SimpleApi.begin('d0', 'dag 0')
        rsrc = Resource('asdf')
        n0 = d0('put_literal', rsrc)
        n1 = d0('put_literal', 1)
        with d0.tx():
            assert isinstance(n0(), Node)
            assert isinstance(n1(), Node)
        waiter, fnapi = d0.start_fn(expr=[n0, n1])
        with d0.tx():
            assert isinstance(waiter(), FnWaiter)
        expr = fnapi('get_expr')
        assert expr == [rsrc, 1]
        n2 = fnapi('put_literal', {'asdf': 128})
        n3 = fnapi('commit', n2)
        dump = api.dump_ref(fnapi.ctx, n3)
        api.load_ref(d0.ctx, dump)
        x = d0('get_fn_result', waiter)
        d0('commit', x)
        resp = d0('get_node_value', x)
        assert resp == {'asdf': 128}

    def test_fn_meta(self):
        d0 = SimpleApi.begin('d0', 'dag 0')
        n0 = d0('put_literal', Resource('asdf'))
        n1 = d0('put_literal', 1)
        waiter, fndb = d0.start_fn(expr=[n0, n1])
        assert fndb('get_fn_meta') == ''
        assert fndb('update_fn_meta', '', 'asdfqwer') is None
        assert fndb('get_fn_meta') == 'asdfqwer'
        with self.assertRaisesRegex(Error, 'old metadata'):
            assert fndb('update_fn_meta', '', 'asdfqwer') is None
        assert fndb('get_fn_meta') == 'asdfqwer'

    def test_cache(self):
        d0 = SimpleApi.begin('d0', 'dag 0')
        waiter, fnapi = d0.start_fn(expr=[d0('put_literal', Resource('asdf')),
                                          d0('put_literal', 1)],
                                    use_cache=True)
        assert d0('get_fn_result', waiter) is None
        fndag = fnapi('commit', fnapi('put_literal', 2))
        dump = api.dump_ref(fnapi.ctx, fndag)
        api.load_ref(d0.ctx, dump)
        n1 = d0('get_fn_result', waiter)
        assert d0('get_node_value', n1) == 2
        d0('populate_cache', waiter)
        waiter, fnapi = d0.start_fn(expr=[d0('put_literal', Resource('asdf')),
                                          d0('put_literal', 1)],
                                    use_cache=True)
        n2 = d0('get_fn_result', waiter)
        assert d0('get_node_value', n2) == 2
