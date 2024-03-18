import unittest
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Node, Ref, Repo, Resource, unroll_datum


def dump(repo, count=None):
    rows = []
    for db in repo.dbs.keys():
        [rows.append([len(rows) + 1, k.to, k()]) for k in repo.cursor(db)]
    rows = rows[:min(count, len(rows))] if count is not None else rows
    print('\n' + tabulate(rows, tablefmt="simple_grid"))

@dataclass
class BasicPyLib:
    d0: TemporaryDirectory = field(default_factory=TemporaryDirectory)
    d1: TemporaryDirectory = field(default_factory=TemporaryDirectory)
    ctx: Config | None = None
    token: Repo | None = None

    @contextmanager
    def init(self, *args):
        with self.d0 as d0, self.d1 as d1:
            self.ctx = Config(
                _CONFIG_DIR=d0,
                _PROJECT_DIR=d1,
                _USER='user0',
            )
            api.create_repo(self.ctx, 'test')
            api.use_repo(self.ctx, 'test')
            api.init_project(self.ctx, 'test')
            if len(args):
                self = self.begin(*args)
            yield self

    def __call__(self, op, *args, **kwargs):
        return api.invoke_api(self.ctx, self.token, [op, args, kwargs])

    def begin(self, *args):
        if self.token is None:
            kw = dict(zip(['name', 'message'], args, strict=True))
        else:
            kw = {'expr': args}
        token = api.invoke_api(self.ctx, self.token, ['begin', [], kw])
        return replace(self, token=token)

    @property
    def tx(self):
        return self.token.tx


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
        inner.start_fn = lambda *expr: self.wrap(inner('start_fn', expr=expr))
        return inner

    def begin(self, name, message):
        tok = api.begin_dag(self.CTX, name, message)
        return self.wrap(tok)

    @contextmanager
    def tx(self, write=False):
        db = Repo(self.CTX.REPO_PATH, self.CTX.USER, self.CTX.BRANCHREF)
        with db.tx(write):
            yield db

    def test_create_dag(self):

        # dag 0
        d0 = self.begin('d0', 'dag 0')
        # d0 = api.begin_dag(self.CTX, 'd0', 'dag 0')
        data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
        # n0 = api.invoke_api(self.CTX, d0, ['put_literal', [data], {}])
        n0 = d0('put_literal', data)
        # api.invoke_api(self.CTX, d0, ['commit', [n0], {}])
        d0('commit', n0)

        # dag 1
        # d0 = api.begin_dag(self.CTX, 'd1', 'dag 1')
        d1 = self.begin('d1', 'dag 1')
        n0 = d1('put_load', 'd0')
        with self.tx():
            assert isinstance(n0(), Node)
            val = n0().value
        n1 = d1('put_literal', [val, val, 2])
        with self.tx():
            assert unroll_datum(n1().value) == [data, data, 2]
        d1('commit', n1)

    def test_fn(self):
        rsrc = Resource('asdf')
        d0 = self.begin('d0', 'dag 0')
        n0 = d0('put_literal', rsrc)
        n1 = d0('put_literal', 1)
        with self.tx():
            assert isinstance(n0(), Node)
            assert isinstance(n1(), Node)
        fn = d0.start_fn(n0, n1)
        expr = fn('get_expr')
        assert expr == [rsrc, 1]
        n2 = fn('put_literal', {'asdf': 128})
        n3 = fn('commit', n2, d0)
        d0('commit', n3)
        resp = api.invoke_api(self.CTX, None, ['get_node_value', [n2], {}])
        assert resp == {'asdf': 128}
