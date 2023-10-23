import unittest
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Error, Fnapp, Node, Literal, Ref, Repo, Resource


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

    def test_create_dag(self):
        ctx = self.CTX

        # dag 0
        tok = api.invoke_api(ctx, None, ['begin', {'name': 'd0', 'message': 'dag 0'}])
        data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
        res = api.invoke_api(ctx, tok, ['put_literal', {'data': data}])
        res = api.invoke_api(ctx, tok, ['commit', {'result': res}])

        # dag 1
        tok = api.invoke_api(ctx, None, ['begin', {'name': 'd1', 'message': 'dag 1'}])
        res = api.invoke_api(ctx, tok, ['put_load', {'dag': 'd0'}])
        with Repo.from_state(tok).tx():
            assert isinstance(res(), Node)
            val = res().value()
        tmp = [val, val, 2]
        res = api.invoke_api(ctx, tok, ['put_literal', {'data': tmp}])
        ref = res
        res = api.invoke_api(ctx, tok, ['get_node', {'ref': ref}])
        db = Repo.from_state(tok)
        with db.tx():
            result = db.get_datum(res.value)
            assert result == [data, data, 2]
        res = api.invoke_api(ctx, tok, ['commit', {'result': ref}])

    def test_fn(self):
        ctx = self.CTX
        tok = api.invoke_api(ctx, None, ['begin', {'name': 'd0', 'message': 'dag 0'}])
        n0 = api.invoke_api(ctx, tok, ['put_literal', {'data': Resource({'asdf', 2})}])
        n1 = api.invoke_api(ctx, tok, ['put_literal', {'data': 1}])
        expr = [n0, n1]
        n2 = api.invoke_api(
            ctx,
            tok,
            ['put_fn', {'expr': expr, 'info': {'foo': 1}}]
        )
        found = None
        try:
            n2 = api.invoke_api(
                ctx,
                tok,
                ['put_fn', {'expr': expr, 'info': {'foo': 2}}],
            )
        except Error as e:
            db = Repo.from_state(tok)
            with db.tx():
                found = e.context['found']
                fnex = found.fnex()
                assert fnex.info == {'foo': 1}
                assert (fnex.value or fnex.error) is None
        n2 = api.invoke_api(
            ctx,
            tok,
            ['put_fn', {'expr': expr, 'info': {'foo': 2}, 'replacing': found}],
        )
        n4 = api.invoke_api(
            ctx,
            tok,
            ['put_fn', {'expr': expr, 'value': {'foo': 2}, 'replacing': n2}],
        )
        api.invoke_api(ctx, tok, ['commit', {'result': n4}])

    def test_fn_w_error(self):
        ctx = self.CTX
        tok = api.invoke_api(ctx, None, ['begin', {'name': 'd0', 'message': 'dag 0'}])
        n0 = api.invoke_api(ctx, tok, ['put_literal', {'data': Resource({'asdf', 2})}])
        n1 = api.invoke_api(ctx, tok, ['put_literal', {'data': 1}])
        error = Error('fooby', {'asdf': 23})
        n2 = api.invoke_api(
            ctx,
            tok,
            ['put_fn', {'expr': [n0, n1], 'error': error}]
        )
        n1 = api.invoke_api(ctx, tok, ['get_node', {'ref': n2}])
        db = Repo.from_state(tok)
        with db.tx():
            assert n1.value is None
            assert n1.error == error

    def test_fn_w_value(self):
        ctx = self.CTX
        tok = api.invoke_api(ctx, None, ['begin', {'name': 'd0', 'message': 'dag 0'}])
        n0 = api.invoke_api(ctx, tok, ['put_literal', {'data': Resource({'asdf', 2})}])
        n1 = api.invoke_api(ctx, tok, ['put_literal', {'data': 1}])
        value = {'asdf': 23}
        n2 = api.invoke_api(
            ctx,
            tok,
            ['put_fn', {'expr': [n0, n1], 'value': value}]
        )
        n1 = api.invoke_api(ctx, tok, ['get_node', {'ref': n2}])
        db = Repo.from_state(tok)
        with db.tx():
            assert isinstance(n1.value().value, dict)
            assert db.get_datum(n1.value().value) == value
