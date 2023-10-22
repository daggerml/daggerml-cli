import unittest
from tempfile import TemporaryDirectory
from tabulate import tabulate
from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Fnapp, Node, Ref, Repo, Resource


def unroll_datum(ref):
    val = ref().value
    if isinstance(val, (bool, int, float, str, Resource)):
        return val
    if isinstance(val, list):
        return [unroll_datum(x) for x in val]
    if isinstance(val, set):
        return {unroll_datum(x) for x in val}
    if isinstance(val, dict):
        return {k: unroll_datum(v) for k, v in val.items()}
    raise RuntimeError(f'unknown type: {type(ref)}')


def dump(repo, count=None):
    rows = []
    for db in repo.dbs.keys():
        [rows.append([len(rows) + 1, k.to, k()]) for k in repo.cursor(db)]
    rows = rows[:min(count, len(rows))] if count is not None else rows
    print('\n' + tabulate(rows, tablefmt="simple_grid"))


class TestApi(unittest.TestCase):

    def setUp(self):
        self.tmpdir_ctx = [TemporaryDirectory(), TemporaryDirectory()]
        self.tmpdirs = [x.__enter__() for x in self.tmpdir_ctx]
        self.CTX = Config(
            self.tmpdirs[0],
            self.tmpdirs[1],
            'repo0',
            'branch0',
            'user0',
        )

    def tearDown(self):
        for x in self.tmpdir_ctx:
            x.__exit__(None, None, None)

    def test_create_dag(self):
        ctx = self.CTX
        api.create_repo(ctx, 'test')
        assert api.list_repo(ctx) == ['test']
        api.use_repo(ctx, 'test')
        assert api.list_branch(ctx) == ['main']
        api.init_project(ctx, 'test')
        api.create_branch(ctx, 'b0')
        assert api.current_branch(ctx) == 'b0'
        resp = api.invoke_api(ctx, None, ['begin', 'd0', 'mee@foo', 'dag 0'])
        assert resp.get('error') is None
        assert resp['status'] == 'ok'
        data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
        resp = api.invoke_api(ctx, resp['token'], ['put_node', 'literal', api.datum2js(data)])
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
        tmp = {
            'type': 'list',
            'value': [
                {'type': 'ref', 'value': resp['result']['ref']},
                {'type': 'ref', 'value': resp['result']['ref']},
            ]
        }
        resp = api.invoke_api(ctx, resp['token'], ['put_node', 'literal', tmp])
        assert resp.get('error') is None
        assert resp['status'] == 'ok'
        assert resp.get('token') is not None
        cres = api.invoke_api(ctx, resp['token'], ['commit', {'ref': resp['result']['ref']}])
        assert cres.get('error') is None
        assert cres['status'] == 'ok'
        db = Repo.from_state(resp['token'])
        with db.tx():
            ref = Ref(resp['result']['ref'])().value
            result = unroll_datum(ref)
            assert result == [data, data]

    def test_fn(self):
        tmp = {
            'type': 'resource',
            'value': {'data': None}
        }
        api.create_repo(self.CTX, self.id())
        ctx = self.CTX.replace()
        api.init_project(ctx, self.id())
        resp = api.invoke_api(ctx, None, ['begin', 'd0', 'mee@foo', 'dag 0'])
        resrc = api.invoke_api(ctx, resp['token'], ['put_node', 'literal', tmp])
        resp0 = api.invoke_api(ctx, resp['token'], ['put_node', 'fn', {'expr': [resrc['result']['ref']]}])
        assert resp0.get('error') is None
        assert resp0.get('token') is not None
        resp1 = api.invoke_api(ctx, resp['token'], ['put_node', 'fn', {'expr': [resrc['result']['ref']]}])
        assert resp0.get('error') is None
        assert resp0.get('token') is not None
        assert resp0 == resp1
