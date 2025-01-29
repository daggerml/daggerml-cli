import os
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Error, Ref, Resource
from daggerml_cli.util import assoc, conj
from tests.util import SimpleApi

SUM = Resource('./tests/fn/sum.py', adapter='dml-python-fork-adapter')


def env(**kwargs):
    return mock.patch.dict(os.environ, **kwargs)


class TestApiCreate(TestCase):

    def test_create_dag(self):
        with TemporaryDirectory() as tmpd0, TemporaryDirectory() as tmpd1:
            ctx = Config(
                _CONFIG_DIR=tmpd0,
                _PROJECT_DIR=tmpd1,
                _USER='user0',
            )
            api.create_repo(ctx, 'test')
            assert api.with_query(api.list_repo, '[*].name')(ctx) == ['test']
            api.config_repo(ctx, 'test')
            assert api.jsdata(api.list_branch(ctx)) == ['main']
            api.create_branch(ctx, 'b0')
            assert api.current_branch(ctx) == 'b0'


class TestApiBase(TestCase):

    def test_create_dag(self):
        with TemporaryDirectory() as config_dir:
            with SimpleApi.begin('d0', config_dir=config_dir) as d0:
                data = {'foo': 23, 'bar': {4, 6}, 'baz': [True, 3]}
                n0 = d0.put_literal(data, name='n0', doc='This is my data.')
                with d0.tx():
                    assert d0.get_node('n0') == n0
                    assert n0().doc == 'This is my data.'
                d0.commit(n0)
            with SimpleApi.begin('d1', config_dir=config_dir) as d1:
                n0 = d1.put_load('d0', name='n0', doc='From dag d0.')
                with d0.tx():
                    assert d1.get_node('n0') == n0
                    assert n0().doc == 'From dag d0.'
                n1 = d1.put_literal([n0, n0, 2])
                assert d1.unroll(n1) == [data, data, 2]
                d1.commit(n1)

    def test_name(self):
        with SimpleApi.begin() as d0:
            n0 = d0.put_literal(42)
            d0.set_node('n0', n0)
            assert d0.get_node('n0') == n0

    def test_fn(self):
        with SimpleApi.begin() as d0:
            result = d0.start_fn(SUM, 1, 2, name='result', doc='I called a func!')
            with d0.tx():
                assert d0.get_node('result') == result
                with self.assertRaises(Error):
                    d0.get_node('BOGUS')
                assert result().doc == 'I called a func!'
            assert d0.unroll(result)[1] == 3

    def test_fn2(self):
        with SimpleApi.begin() as d0:
            result = d0.start_fn(SUM, 1, 2, name='result', doc='I called a func!')
            with d0.tx():
                assert d0.get_node('result') == result
                with self.assertRaises(Error):
                    d0.get_node('BOGUS')
                assert result().doc == 'I called a func!'
            assert d0.unroll(result)[1] == 3

    def test_repo_cache(self):
        argv = [SUM, 1, 2]
        with SimpleApi.begin() as d0:
            res0 = d0.unroll(d0.start_fn(*argv))
            res1 = d0.unroll(d0.start_fn(*argv))
            assert res0 == res1
            assert res0[1] == 3

    def test_fn_nocache(self):
        argv = [SUM, 1, 2]

        with SimpleApi.begin() as d0:
            res0 = d0.unroll(d0.start_fn(*argv))

        with SimpleApi.begin() as d0:
            res1 = d0.unroll(d0.start_fn(*argv))

        assert res0 != res1

    def test_fn_cache(self):
        argv = [SUM, 1, 2]

        with TemporaryDirectory() as fn_cache_dir:
            with SimpleApi.begin(fn_cache_dir=fn_cache_dir) as d0:
                res0 = d0.unroll(d0.start_fn(*argv))

            with SimpleApi.begin(fn_cache_dir=fn_cache_dir) as d0:
                res1 = d0.unroll(d0.start_fn(*argv))

            assert res0 == res1

    def test_fn_error(self):
        argv = [SUM, 1, 2, 'BOGUS']

        with env(DML_FN_FILTER_ARGS='True'):
            with SimpleApi.begin() as d0:
                assert d0.unroll(d0.start_fn(*argv))[1] == 3

        with TemporaryDirectory() as config_dir:
            with self.assertRaises(Error):
                with SimpleApi.begin(config_dir=config_dir) as d0:
                    d0.start_fn(*argv)

            with env(DML_FN_FILTER_ARGS='True'):
                with self.assertRaises(Error):
                    with SimpleApi.begin(config_dir=config_dir) as d0:
                        d0.start_fn(*argv)

                with SimpleApi.begin(config_dir=config_dir) as d0:
                    res0 = d0.start_fn(*argv, retry=True)
                    assert d0.unroll(res0)[1] == 3

                with SimpleApi.begin(config_dir=config_dir) as d0:
                    assert d0.start_fn(*argv) == res0

    def test_specials(self):
        with SimpleApi.begin() as d0:
            def check_len(n, v):
                assert d0.unroll(d0.len(n)) == len(v)
            def check_keys(n, v):
                assert d0.unroll(d0.keys(n)) == sorted(v.keys())
            def check_contains(n, v):
                for i in v:
                    assert d0.unroll(d0.contains(n, d0.put_literal(i)))
                assert not d0.unroll(d0.contains(n, d0.put_literal('BOGUS')))
            def check_list_get(n, v):
                for i in range(len(v)):
                    assert d0.unroll(d0.get(n, d0.put_literal(i))) == v[i]
                with self.assertRaises(Error):
                    d0.unroll(d0.get(n, d0.put_literal(len(v))))
            def check_slice(n, v, *k):
                assert d0.unroll(d0.get(n, d0.put_literal(list(k)))) == v[slice(*k)]
            def check_dict_get(n, v):
                for i in v:
                    assert d0.unroll(d0.get(n, d0.put_literal(i)))
                with self.assertRaises(Error):
                    d0.unroll(d0.get(n, d0.put_literal('BOGUS')))
            def check_assoc(n, v, k, x):
                assert d0.unroll(d0.assoc(n, k, x)) == assoc(v, k, x)
            def check_conj(n, v, x):
                assert d0.unroll(d0.conj(n, x)) == conj(v, x)
            x0 = {
                'list': [1, 2, 3],
                'set': {1, 2, 3},
                'dict': {'x': 1, 'y': 2, 'z': 3},
                'int': 0,
                'float': 0.1,
                'bool': True,
                'NoneType': None,
                'Resource': Resource('test')
            }
            n0 = d0.put_literal(x0)
            for k, v in x0.items():
                n = d0.get(n0, d0.put_literal(k), name='n', doc='a node')
                with d0.tx():
                    # FIXME: test node name
                    # assert n().name == 'n'
                    assert n().doc == 'a node'
                assert d0.unroll(n) == v
                assert d0.unroll(d0.type(n)) == k
                if k == "list":
                    check_len(n, v)
                    check_list_get(n, v)
                    check_contains(n, v)
                    check_conj(n, v, 4)
                    check_assoc(n, v, 0, 0)
                    check_slice(n, v, 1, None, None)
                elif k == 'set':
                    check_len(n, v)
                    check_contains(n, v)
                    check_conj(n, v, 4)
                elif k == 'dict':
                    check_len(n, v)
                    check_keys(n, v)
                    check_dict_get(n, v)
                    check_contains(n, v)
                    check_assoc(n, v, 'x', 0)

            assert d0.unroll(d0.list()) == []
            assert d0.unroll(d0.list(0, 1, 2, 3)) == [0, 1, 2, 3]

            assert d0.unroll(d0.dict()) == {}
            assert d0.unroll(d0.dict('x', 1, 'y', 2, 'z', 3)) == {'x': 1, 'y': 2, 'z': 3}

            assert d0.unroll(d0.set()) == set()
            assert d0.unroll(d0.set(0, 1, 2, 3)) == {0, 1, 2, 3}

            assert d0.unroll(d0.build([1, {'x': 42}], d0.put_literal(42))) == [1, {'x': 42}]

    def test_describe_dag(self):
        with TemporaryDirectory() as config_dir:
            with SimpleApi.begin('d0', config_dir=config_dir) as d0:
                d0.commit(d0.put_literal(23))
            with SimpleApi.begin('d1', config_dir=config_dir) as d1:
                nodes = [
                    d1.put_literal(SUM),
                    d1.put_load('d0'),
                    d1.put_literal(23),
                ]
                result = d1.start_fn(*nodes)
                assert d1.unroll(result)[1] == 46
                d1.commit(result)
            ref, = (x.id.id for x in api.list_dags(d1.ctx) if x.name == 'd1')
            desc = api.describe_dag(d1.ctx, Ref(f'dag/{ref}'))
            assert len(desc["edges"]) == len(nodes) + 1  # +1 because dag->node edge
            assert {e["source"] for e in desc["edges"] if e["type"] == "node"} == {x for x in nodes}
