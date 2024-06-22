#!/usr/bin/env python3
import unittest
from collections import Counter
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli.repo import (
    Ctx,
    Error,
    Literal,
    Load,
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
        return Ctx.from_head(db.head).dags[dag]

    def dag_result(self, dag):
        return unroll_datum(dag().result().value)

    def test_create_dag(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index, dag = db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum(Resource('a'))), index=index, dag=dag)
            db.commit(n0, index=index)

    def test_update_fn_meta(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True), TemporaryDirectory() as tmpd:
            index, dag = db.begin(name='d0', message='1st dag')
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            expr = [db.put_node(x, index=index, dag=dag) for x in expr]
            # start without cache
            _fndag, fnidx = db.start_fn(expr=expr, index=index, cache=False, path=tmpd)
            db2 = Repo(tmpd)
            with db2.tx(True):
                fndag = fnidx().dag
                assert db2.get_fn_meta(fndag) == ''
                assert db2.get_fn_meta(fndag) == fndag().meta
                assert db2.update_fn_meta(fndag, '', 'asdf') is None
                assert db2.get_fn_meta(fndag) == 'asdf'
                with self.assertRaisesRegex(Error, 'old metadata'):
                    assert db2.update_fn_meta(fndag, 'wrong-value', 'qwer') is None
                assert db2.get_fn_meta(fndag) == 'asdf'

    def test_cache_basic(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index, dag = db.begin(name='d0', message='1st dag')
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            expr = [db.put_node(x, index=index, dag=dag) for x in expr]
            # start without cache
            with TemporaryDirectory() as tmpd:
                fndag, fnidx = db.start_fn(expr=expr, index=index, cache=False, path=tmpd)
                assert fndag.type == 'fndag'
                res = db.put_node(Literal(db.put_datum('omg')), index=index, dag=fndag)
                node = db.commit(res, index=index, parent_dag=dag)
            assert node.type == 'node'
            assert node().value().value == 'omg'
            # retry and get a cached dag instance
            with TemporaryDirectory() as tmpd:
                fndag, *_ = db.start_fn(expr=expr, index=index, cache=True, path=tmpd)
                assert fndag.type == 'cachedfndag'
                node = db.put_node(Literal(db.put_datum('zmg')), index=index, dag=fndag)
                node = db.commit(node, index=index, parent_dag=dag, cache=True)
            assert node.type == 'node'
            assert node().value().value == 'zmg'
            # result should be cached
            with TemporaryDirectory() as tmpd:
                node = db.start_fn(expr=expr, index=index, cache=True, path=tmpd)
                assert node.type == 'node'
                assert node().value().value == 'zmg'

    def test_cache_replace(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index, dag = db.begin(name='d0', message='1st dag')
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            expr = [db.put_node(x, index=index, dag=dag) for x in expr]
            # start a cached run
            with TemporaryDirectory() as tmpd:
                fndag, *_ = db.start_fn(expr=expr, index=index, cache=True, path=tmpd)
                assert fndag.type == 'cachedfndag'
                node = db.put_node(Literal(db.put_datum('zomg')), index=index, dag=fndag)
                node = db.commit(node, index, parent_dag=dag, cache=True)
            with TemporaryDirectory() as tmpd:
                # replace a cached result
                fndag, *_ = db.start_fn(expr=expr, index=index, cache=True, retry=True, path=tmpd)
                node = db.put_node(Literal(db.put_datum(1)), index=index, dag=fndag)
                node = db.commit(node, index, parent_dag=dag, cache=True)
            assert node().value().value == 1
            # new result should be cached
            with TemporaryDirectory() as tmpd:
                node = db.start_fn(expr=expr, index=index, cache=True, path=tmpd)
            assert node.type == 'node'
            assert node().value().value == 1

    def test_cache_delete(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index, dag = db.begin(name='d0', message='1st dag')
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            expr = [db.put_node(x, index=index, dag=dag) for x in expr]
            # start a cached run
            with TemporaryDirectory() as tmpd:
                fndag, *_ = db.start_fn(expr=expr, index=index, cache=True, path=tmpd)
                assert fndag.type == 'cachedfndag'
                node = db.put_node(Literal(db.put_datum('zomg')), index=index, dag=fndag)
                node = db.commit(node, index, parent_dag=dag, cache=False)
            assert node.type == 'node'
            assert node().value().value == 'zomg'
            # no result cached
            with TemporaryDirectory() as tmpd:
                fndag, *_ = db.start_fn(expr=expr, index=index, cache=True, path=tmpd)
                assert fndag.type == 'cachedfndag'

    def test_cache_newdag(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            index, dag = db.begin(name='d0', message='1st dag')
            assert dag.type == 'dag'
            # start a cached run
            expr = [db.put_node(x, index=index, dag=dag) for x in expr]
            with TemporaryDirectory() as tmpd:
                fndag, fnidx = db.start_fn(expr=expr, index=index, cache=True, path=tmpd)
                db2 = Repo(tmpd)
                with db2.tx(True):
                    node = db2.put_node(Literal(db2.put_datum('zomg')),
                                        index=fnidx, dag=fndag)
                    node = db2.commit(node, fnidx, parent_dag=dag, cache=True)
            db.commit(node, index=index)
        with db.tx(True):
            expr = [
                Literal(db.put_datum(Resource(self.id()))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            # new result should be cached
            index, dag = db.begin(name='d1', message='2nd dag')
            expr = [db.put_node(x, index=index, dag=dag) for x in expr]
            with TemporaryDirectory() as tmpd:
                node = db.start_fn(expr=expr, index=index, cache=True, path=tmpd)
                assert node.type == 'node'
                assert node().value().value == 'zomg'

    def test_get_nodeval(self):
        data = {'asdf': 23}
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index, dag = db.begin(name='d0', message='1st dag')
            node = db.put_node(Literal(db.put_datum(data)), index=index, dag=dag)
            res = db.get_node_value(node)
            assert res == data

    def test_walk(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        def walk_types(obj):
            return Counter(x.type for x in db.walk(obj))
        with db.tx(True):
            index, dag = db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum({'foo': 42})), index=index, dag=dag)
            db.commit(n0, index=index)
            assert walk_types(self.get_dag(db, 'd0')) == {'dag': 1, 'datum': 2, 'node': 1}
            assert walk_types(self.get_dag(db, 'd0')().result) == {'datum': 2, 'node': 1}
            assert walk_types(self.get_dag(db, 'd0')().result().value) == {'datum': 2}

    def test_walk_ordered(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        def walk_types(obj, ordered=False):
            f = db.walk_ordered if ordered else db.walk
            return Counter(x.type for x in f(obj))
        with db.tx(True):
            index, dag = db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum({'foo': 42})), index=index, dag=dag)
            db.commit(n0, index=index)
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
            index, dag = db.begin(name='d0', message='1st dag')
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
                ref = db.put_node(Literal(db.put_datum(v)), index=index, dag=dag)
                assert isinstance(ref, Ref)
                node = ref()
                assert isinstance(node, Node)
                val = unroll_datum(node.value())
                assert val == v, f'failed {k}'

    def test_dag_dump(self):
        rsrc = Resource('asdf')
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index, dag = db.begin(name='d0', message='1st dag')
            node0 = db.put_node(Literal(db.put_datum(rsrc)), index=index, dag=dag)
        with TemporaryDirectory() as tmpd:
            new_index = db.dump_dag(dag, tmpd, name='fn', create=True)
            repo = Repo(tmpd)
            with repo.tx(True):
                assert repo.get_node_value(node0)  == rsrc
                node1 = repo.put_node(Literal(repo.put_datum(23)),
                                      index=new_index, dag=new_index().dag)
                assert repo.get_node_value(node1)  == 23
        with db.tx():
            with self.assertRaisesRegex(AssertionError, 'invalid type'):
                db.get_node_value(node1)

    def test_dag_dump_n_load(self):
        rsrc = Resource('asdf')
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index, dag = db.begin(name='d0', message='1st dag')
            node0 = db.put_node(Literal(db.put_datum(rsrc)), index=index, dag=dag)
        with TemporaryDirectory() as tmpd:
            new_index = db.dump_dag(dag, tmpd, name='fn', create=True)
            repo = Repo(tmpd)
            with repo.tx(True):
                new_dag = new_index().dag
                node1 = repo.put_node(Literal(repo.put_datum(23)),
                                      index=new_index, dag=new_dag)
                repo.commit(node1, new_index)
            final_dag = db.load_dag(tmpd, new_dag)
        with db.tx():
            assert db.get_node_value(final_dag().result) == 23
