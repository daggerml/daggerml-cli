#!/usr/bin/env python3
import unittest
from collections import Counter
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli.repo import (
    Error,
    Literal,
    Node,
    Ref,
    Repo,
    Resource,
    from_json,
    to_json,
    unroll_datum,
)


def dump(repo, count=None):
    rows = []
    for db in repo.dbs.keys():
        [rows.append([len(rows) + 1, k.to, k()]) for k in repo.cursor(db)]
    rows = rows[:min(count, len(rows))] if count is not None else rows
    print('\n' + tabulate(rows, tablefmt="simple_grid"))


class TestRepo(unittest.TestCase):

    def setUp(self):
        self.tmpdir_ctx = TemporaryDirectory()
        self.tmpdir = self.tmpdir_ctx.__enter__()

    def tearDown(self):
        self.tmpdir_ctx.__exit__(None, None, None)

    def get_dag(self, db, dag):
        return db.ctx(db.head).dags[dag]

    def dag_result(self, dag):
        return unroll_datum(dag().result().value)

    def test_create_dag(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            dag = db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum(Resource('a'))), index=dag)
            db.commit(n0, index=dag)

    def test_update_fn_meta(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            dag = db.begin(name='d0', message='1st dag')
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            expr = [db.put_node(x, index=dag) for x in expr]
            # start without cache
            fndag = db.start_fn(expr=expr, index=dag, cache=False)
            fndag = fndag().dag
            assert db.get_fn_meta(fndag) == ''
            assert db.get_fn_meta(fndag) == fndag().meta
            assert db.update_fn_meta(fndag, '', 'asdf') is None
            assert db.get_fn_meta(fndag) == 'asdf'
            with self.assertRaisesRegex(Error, 'old metadata'):
                assert db.update_fn_meta(fndag, 'wrong-value', 'qwer') is None
            assert db.get_fn_meta(fndag) == 'asdf'

    def test_cache_basic(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            dag = db.begin(name='d0', message='1st dag')
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            expr = [db.put_node(x, index=dag) for x in expr]
            # start without cache
            fndag = db.start_fn(expr=expr, index=dag, cache=False)
            assert fndag.type == 'index'
            assert fndag().dag.type == 'fndag'
            res = db.put_node(Literal(db.put_datum('omg')), index=fndag)
            node = db.commit(res, index=fndag)
            assert node.type == 'node'
            assert node().value().value == 'omg'
            # retry and get a cached dag instance
            fndag = db.start_fn(expr=expr, index=dag, cache=True)
            assert fndag.type == 'index'
            assert fndag().dag.type == 'cachedfndag'
            node = db.commit(db.put_node(Literal(db.put_datum('zomg')), index=fndag), index=fndag, cache=True)
            assert node.type == 'node'
            assert node().value().value == 'zomg'
            # result should be cached
            node = db.start_fn(expr=expr, index=dag, cache=True)
            assert node.type == 'node'
            assert node().value().value == 'zomg'

    def test_cache_replace(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            dag = db.begin(name='d0', message='1st dag')
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            expr = [db.put_node(x, index=dag) for x in expr]
            # start a cached run
            fndag = db.start_fn(expr=expr, index=dag, cache=True)
            assert fndag.type == 'index'
            assert fndag().dag.type == 'cachedfndag'
            node = db.commit(db.put_node(Literal(db.put_datum('zomg')), index=fndag), index=fndag, cache=True)
            # replace a cached result
            fndag = db.start_fn(expr=expr, index=dag, cache=True, retry=True)
            node = db.commit(db.put_node(Literal(db.put_datum(1)), index=fndag), index=fndag, cache=True)
            assert node().value().value == 1
            # new result should be cached
            node = db.start_fn(expr=expr, index=dag, cache=True)  # we should have a cached result now
            assert node.type == 'node'
            assert node().value().value == 1

    def test_cache_delete(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            dag = db.begin(name='d0', message='1st dag')
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            expr = [db.put_node(x, index=dag) for x in expr]
            # start a cached run
            fndag = db.start_fn(expr=expr, index=dag, cache=True)
            assert fndag.type == 'index'
            assert fndag().dag.type == 'cachedfndag'
            node = db.commit(db.put_node(Literal(db.put_datum('zomg')), index=fndag), index=fndag, cache=False)
            assert node.type == 'node'
            assert node().value().value == 'zomg'
            # no result cached
            fndag = db.start_fn(expr=expr, index=dag, cache=True)
            assert fndag.type == 'index'
            assert fndag().dag.type == 'cachedfndag'

    def test_cache_newdag(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            dag = db.begin(name='d0', message='1st dag')
            assert dag().dag.type == 'dag'
            # start a cached run
            fndag = db.start_fn(expr=[db.put_node(x, index=dag) for x in expr], index=dag, cache=True)
            node = db.put_node(Literal(db.put_datum('zomg')), index=fndag)
            node = db.commit(node, index=fndag, cache=True)
            db.commit(node, index=dag)
        with db.tx(True):
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            # new result should be cached
            dag = db.begin(name='d1', message='2nd dag')
            node = db.start_fn(expr=[db.put_node(x, index=dag) for x in expr], index=dag, cache=True)
            assert node.type == 'node'
            assert node().value().value == 'zomg'

    def test_get_nodeval(self):
        data = {'asdf': 23}
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            dag = db.begin(name='d0', message='1st dag')
            node = db.put_node(Literal(db.put_datum(data)), index=dag)
            res = db.get_node_value(node)
            assert res == data

    def test_walk(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        def walk_types(obj):
            return Counter(x.type for x in db.walk(obj))
        with db.tx(True):
            dag = db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum({'foo': 42})), index=dag)
            db.commit(n0, index=dag)
            assert walk_types(self.get_dag(db, 'd0')) == {'dag': 1, 'datum': 2, 'node': 1}
            assert walk_types(self.get_dag(db, 'd0')().result) == {'datum': 2, 'node': 1}
            assert walk_types(self.get_dag(db, 'd0')().result().value) == {'datum': 2}

    def test_walk_ordered(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        def walk_types(obj, ordered=False):
            f = db.walk_ordered if ordered else db.walk
            return Counter(x.type for x in f(obj))
        with db.tx(True):
            dag = db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum({'foo': 42})), index=dag)
            db.commit(n0, index=dag)
            _dag = self.get_dag(db, 'd0')
            assert walk_types(_dag) == {'dag': 1, 'datum': 2, 'node': 1}
            assert walk_types(_dag().result) == {'datum': 2, 'node': 1}
            assert walk_types(self.get_dag(db, 'd0')().result().value) == {'datum': 2}
            assert db.walk(self.get_dag(db, 'd0')) == set(db.walk_ordered(self.get_dag(db, 'd0')))
            self.assertCountEqual(
                list(db.walk(self.get_dag(db, 'd0'))),
                db.walk_ordered(self.get_dag(db, 'd0')),
            )

    def test_datatypes(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            dag = db.begin(name='d0', message='1st dag')
            data = {
                'int': 23,
                'float': 12.43,
                'bool': True,
                'null': None,
                'string': 'qwer',
                'list': [3, 4, 5],
                'map': {'a': 2, 'b': 'asdf'},
                'set': {12, 13, 'a', 3.4},
                'resource': Resource('qwer'),
                'composite': {'asdf': {2, Resource('qwer')}},
            }
            for k, v in data.items():
                assert from_json(to_json(v)) == v
                ref = db.put_node(Literal(db.put_datum(v)), index=dag)
                assert isinstance(ref, Ref)
                node = ref()
                assert isinstance(node, Node)
                val = unroll_datum(node.value())
                assert val == v, f'failed {k}'
