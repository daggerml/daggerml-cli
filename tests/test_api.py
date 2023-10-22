import unittest
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import DATA_TYPE, Error, Fnapp, Node, Ref, Repo, Resource


def unroll_datum(val):
    if isinstance(val, Ref):
        return unroll_datum(val())
    val = val.value
    if isinstance(val, (bool, int, float, str, Resource)):
        return val
    if isinstance(val, list):
        return [unroll_datum(x) for x in val]
    if isinstance(val, set):
        return {unroll_datum(x) for x in val}
    if isinstance(val, dict):
        return {k: unroll_datum(v) for k, v in val.items()}
    raise RuntimeError(f'unknown type: {type(val)}')


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
        assert resp.get('token') is not None
        # with Repo.from_state(resp['token']).tx():
        cres = api.invoke_api(ctx, resp['token'], ['commit', {'ref': resp['result']['ref']}])
        assert cres.get('error') is None
        assert cres['status'] == 'ok'
        db = Repo.from_state(resp['token'])
        with db.tx():
            ref = Ref(resp['result']['ref'])().value
            result = unroll_datum(ref)
            assert result == [data, data, 2]

    def test_fn(self):
        ctx = self.CTX
        resp = api.invoke_api(ctx, None, ['begin', 'd0', 'mee@foo', 'dag 0'])
        resrc = api.invoke_api(ctx, resp['token'], ['put_node', 'literal', api.to_data(['asdf', 2])])
        resp0 = api.invoke_api(ctx, resp['token'], ['put_node', 'fn', {'expr': [resrc['result']['ref']]}])
        assert resp0.get('error') is None
        assert resp0.get('token') is not None
        resp1 = api.invoke_api(ctx, resp['token'], ['put_node', 'fn', {'expr': [resrc['result']['ref']]}])
        assert resp0.get('error') is None
        assert resp0.get('token') is not None
        assert resp0 == resp1

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
        with db.tx():
            assert unroll_datum(res['value']) == value
