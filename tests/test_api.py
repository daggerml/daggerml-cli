import unittest
from contextlib import contextmanager
from dataclasses import dataclass, field
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
    def begin(cls, name=None, message=None, ctx=None, dag_dump=None):
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
        tok = api.begin_dag(ctx, name=name, message=message, dag_dump=dag_dump)
        return cls(tok, ctx, tmpdirs)

    @contextmanager
    def tx(self, write=False):
        db = Repo(self.ctx.REPO_PATH, self.ctx.USER, self.ctx.BRANCHREF)
        with db.tx(write):
            yield db

    def cleanup(self):
        for x in self.tmpdirs:
            x.__exit__(None, None, None)

    def start_fn(self, expr):
        waiter, cache_key, dump = self('start_fn', expr)
        fnapi = SimpleApi.begin('dag', 'message', dag_dump=dump)
        return waiter, fnapi

    def __enter__(self):
        return self

    def __exit__(self, *x):
        self.cleanup()


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
        with SimpleApi.begin(name='d0', message='dag 0') as d0:
            data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
            n0 = d0('put_literal', data)
            d0('commit', n0)
            # dag 1
            d1 = SimpleApi.begin(name='d1', message='dag 1', ctx=d0.ctx)
            n0 = d1('put_load', 'd0')
            with d1.tx():
                assert isinstance(n0(), Node)
                val = n0().value
            n1 = d1('put_literal', [val, val, 2])
            with d1.tx():
                assert unroll_datum(n1().value) == [data, data, 2]
            d1('commit', n1)

    def test_fn(self):
        with SimpleApi.begin('d0', 'dag 0') as d0:
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
            assert expr.type == 'node'
            assert d0('unroll', expr) == [rsrc, 1]
            n2 = fnapi('put_literal', {'asdf': 128})
            n3 = fnapi('commit', n2)
            dump = api.dump_ref(fnapi.ctx, n3)
            api.load_ref(d0.ctx, dump)
            x = d0('get_fn_result', waiter)
            d0('commit', x)
            resp = d0('get_node_value', x)
            assert resp == {'asdf': 128}

    def test_cache(self):
        with SimpleApi.begin('d0', 'dag 0') as d0:
            waiter, fnapi = d0.start_fn(expr=[d0('put_literal', Resource('asdf')),
                                              d0('put_literal', 1)])
            assert d0('get_fn_result', waiter) is None
            fndag = fnapi('commit', fnapi('put_literal', 2))
            dump = api.dump_ref(fnapi.ctx, fndag)
            api.load_ref(d0.ctx, dump)
            n1 = d0('get_fn_result', waiter)
            assert d0('get_node_value', n1) == 2
            waiter, fnapi = d0.start_fn(expr=[d0('put_literal', Resource('asdf')),
                                              d0('put_literal', 1)])
            n2 = d0('get_fn_result', waiter)
            assert d0('get_node_value', n2) == 2


class TestSpecials(unittest.TestCase):

    @staticmethod
    def call(api, expr):
        waiter, fnapi = api.start_fn(expr=expr)
        n1 = api('get_fn_result', waiter)
        if isinstance(n1, Error):
            raise n1
        return api('get_node_value', n1)

    def test_len_list(self):
        with SimpleApi.begin('d0', 'dag 0') as d0:
            n0 = d0('put_literal', [1, 2])
            rsrc = d0('put_literal', Resource('/daggerml/len'))
            assert self.call(d0, [rsrc, n0]) == 2

    def test_len_map(self):
        with SimpleApi.begin('d0', 'dag 0') as d0:
            n0 = d0('put_literal', {'a': 1, 'c': 48, 'b': 2})
            rsrc = d0('put_literal', Resource('/daggerml/len'))
            assert self.call(d0, [rsrc, n0]) == 3

    def test_keys_map(self):
        with SimpleApi.begin('d0', 'dag 0') as d0:
            n0 = d0('put_literal', {'a': 1, 'c': 48, 'b': 2})
            rsrc = d0('put_literal', Resource('/daggerml/keys'))
            assert self.call(d0, [rsrc, n0]) == ['a', 'b', 'c']

    def test_get_list(self):
        with SimpleApi.begin('d0', 'dag 0') as d0:
            n0 = d0('put_literal', [1, 2])
            rsrc = d0('put_literal', Resource('/daggerml/get'))
            assert self.call(d0, [rsrc, n0, d0('put_literal', 0)]) == 1

    def test_get_map(self):
        with SimpleApi.begin('d0', 'dag 0') as d0:
            n0 = d0('put_literal', [1, 2])
            rsrc = d0('put_literal', Resource('/daggerml/get'))
            assert self.call(d0, [rsrc, n0, d0('put_literal', 0)]) == 1
            with self.assertRaises(Error):
                self.call(d0, [rsrc, n0, d0('put_literal', 2)])

    def test_error(self):
        with SimpleApi.begin('d0', 'dag 0') as d0:
            n0 = d0('put_literal', [1, 2])
            rsrc = d0('put_literal', Resource('/daggerml/asdfqwefr'))
            with self.assertRaises(Error):
                self.call(d0, [rsrc, n0])

    def test_type(self):
        with SimpleApi.begin('d0', 'dag 0') as d0:
            rsrc = d0('put_literal', Resource('/daggerml/type'))
            def doit(x):
                n0 = d0('put_literal', x)
                return self.call(d0, [rsrc, n0])
            assert doit({'a': 2}) == 'dict'
            assert doit(['a', 2]) == 'list'
            assert doit('a') == 'str'
            assert doit(2) == 'int'
