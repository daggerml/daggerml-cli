import unittest
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Error, Fnapp, Node, Ref, Repo, Resource


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
                tmpd0,
                tmpd1,
                'repo0',
                'branch0',
                'user0',
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
            self.tmpdirs[0],
            self.tmpdirs[1],
            'repo0',
            'branch0',
            'user0',
        )
        api.create_repo(ctx, 'test')
        api.use_repo(ctx, 'test')
        api.init_project(ctx, 'test')

    def tearDown(self):
        for x in self.tmpdir_ctx:
            x.__exit__(None, None, None)

    def test_create_dag(self):
        ctx = self.CTX
        resp = api.invoke_api(ctx, None, ['begin', 'd0', 'mee@foo', 'dag 0'])
        assert resp.get('error') is None
        assert resp['status'] == 'ok'
        data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
        resp = api.invoke_api(ctx, resp['token'], ['put_node', 'literal', api.to_data(data)])
        assert resp.get('error') is None
        assert resp['status'] == 'ok'
        assert resp.get('token') is not None
        resp = api.invoke_api(ctx, resp['token'], ['commit', {'ref': resp['result']['ref']}])
        assert resp.get('error') is None
        assert resp['status'] == 'ok'
        # dag 2
        resp = api.invoke_api(ctx, None, ['begin', 'd1', 'mee@foo', 'dag 1'])
        resp = api.invoke_api(ctx, resp['token'], ['put_node', 'load', 'd0'])
        with Repo.from_state(resp['token']).tx():
            assert isinstance(Ref(resp['result']['ref'])(), Node)
            val = Ref(resp['result']['ref'])().value()
        tmp = [val, val, 2]
        resp = api.invoke_api(ctx, resp['token'], ['put_node', 'literal', api.to_data(tmp)])
        assert resp.get('error') is None
        assert resp['status'] == 'ok'
        node_id = resp['result']['ref']
        resp = api.invoke_api(ctx, resp['token'], ['get_node', node_id])
        res = api.from_data(resp['result'])
        with Repo.from_state(resp['token']).tx():
            result = api.unroll_datum(res['value'])
            assert result == [data, data, 2]
        cres = api.invoke_api(ctx, resp['token'], ['commit', {'ref': node_id}])
        assert cres.get('error') is None
        assert cres['status'] == 'ok'

    def test_fn(self):
        ctx = self.CTX
        resp = api.invoke_api(ctx, None, ['begin', 'd0', 'mee@foo', 'dag 0'])
        n0 = api.invoke_api(ctx, resp['token'], ['put_node', 'literal', api.to_data(Resource({'asdf', 2}))])
        n1 = api.invoke_api(ctx, n0['token'], ['put_node', 'literal', api.to_data(1)])
        expr = [n0['result']['ref'], n1['result']['ref']]
        n2 = api.invoke_api(
            ctx, n1['token'],
            ['put_node', 'fn', {'expr': expr, 'info': {'foo': 1}}]
        )
        assert n2.get('error') is None
        n3 = api.invoke_api(
            ctx, n2['token'],
            ['put_node', 'fn', {'expr': expr, 'info': {'foo': 2}}],
        )
        err_ctx = api.from_data(n3['error']['context'])
        assert err_ctx['info'] == {'foo': 1}
        assert not err_ctx['has_error']
        assert not err_ctx['has_value']
        n3 = api.invoke_api(
            ctx, n2['token'],
            ['put_node', 'fn', {'expr': expr, 'info': {'foo': 2}, 'replace': err_ctx['replace']}],
        )
        n3 = api.invoke_api(
            ctx, n3['token'],
            ['put_node', 'fn', {'expr': expr, 'value': {'foo': 2}, 'replace': n3['result']['replace']}],
        )
        assert n3.get('error') is None
        assert n3.get('token') is not None
        resp = api.invoke_api(ctx, n3['token'], ['commit', {'ref': n3['result']['ref']}])

    def test_fn_w_error(self):
        ctx = self.CTX
        resp = api.invoke_api(ctx, None, ['begin', 'd0', 'mee@foo', 'dag 0'])
        n0 = api.invoke_api(ctx, resp['token'], ['put_node', 'literal', api.to_data(Resource({'asdf', 2}))])
        n1 = api.invoke_api(ctx, n0['token'], ['put_node', 'literal', api.to_data(1)])
        error = Error('fooby', {'asdf': 23})
        n2 = api.invoke_api(
            ctx, resp['token'],
            ['put_node', 'fn', {'expr': [n0['result']['ref'], n1['result']['ref']], 'error': error}]
        )
        n1 = api.invoke_api(ctx, n2['token'], ['get_node', n2['result']['ref']])
        res = api.from_data(n1['result'])
        assert res['error'] == error
        assert res['value'] is None

    def test_fn_w_value(self):
        ctx = self.CTX
        resp = api.invoke_api(ctx, None, ['begin', 'd0', 'mee@foo', 'dag 0'])
        n0 = api.invoke_api(ctx, resp['token'], ['put_node', 'literal', api.to_data(Resource({'asdf', 2}))])
        n1 = api.invoke_api(ctx, n0['token'], ['put_node', 'literal', api.to_data(1)])
        value = {'asdf': 23}
        n2 = api.invoke_api(
            ctx, resp['token'],
            ['put_node', 'fn', {'expr': [n0['result']['ref'], n1['result']['ref']], 'value': api.to_data(value)}]
        )
        n1 = api.invoke_api(ctx, n2['token'], ['get_node', n2['result']['ref']])
        res = api.from_data(n1['result'])
        assert res['error'] is None
        db = Repo.from_state(resp['token'])
        assert isinstance(res['value'].value, dict)
        with db.tx():
            assert api.unroll_datum(res['value']) == value
