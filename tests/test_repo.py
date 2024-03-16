import re
import unittest
from collections import Counter
from tempfile import TemporaryDirectory

import pytest
from tabulate import tabulate

from daggerml_cli.repo import (
    CachedFnDag,
    FnDag,
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
        return db.ctx(db.head).dags[dag]

    def dag_result(self, dag):
        return unroll_datum(dag().result().value)

    def test_create_dag(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum(Resource({'foo': 42}))))
            db.commit(n0)

    def test_cache_basic(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            expr = [
                Literal(db.put_datum(Resource({'foo': 42}))),
                Literal(db.put_datum(['howdy', 1])),
                Literal(db.put_datum(2)),
            ]
            db.begin(name='d0', message='1st dag')
            with self.assertLogs('daggerml_cli.repo', level='DEBUG') as cm:
                db.start_fn(expr=[db.put_node(x) for x in expr], cache=False)
            assert re.match(r'.*starting new fndag.*', cm.output[0])
            assert len(cm.output) == 1
            assert isinstance(db.dag(), FnDag)
            with self.assertLogs('daggerml_cli.repo', level='DEBUG') as cm:
                n0 = db.commit(db.put_node(Literal(db.put_datum('omg'))))
            assert re.match(r'.*commit called with FnDag.*', cm.output[0])
            assert len(cm.output) == 1
            assert isinstance(n0, Ref)
            assert n0.type == 'node'
            with self.assertLogs('daggerml_cli.repo', level='DEBUG') as cm:
                db.start_fn(expr=[db.put_node(x) for x in expr], cache=True)  # fn application, ops on db are now applied to the fndag
            assert re.match(r'.*starting new fndag.*', cm.output[0])
            assert re.match(r'.*populating cache.*', cm.output[1])
            assert isinstance(db.dag(), CachedFnDag)
            with self.assertLogs('daggerml_cli.repo', level='DEBUG') as cm:
                n0 = db.commit(db.put_node(Literal(db.put_datum('zomg'))), cache=True)
            assert any(re.match(r'.*commit called with CachedFnDag.*', o) for o in cm.output)
            assert isinstance(n0, Ref)
            assert n0.type == 'node'
            with self.assertLogs('daggerml_cli.repo', level='DEBUG') as cm:
                n1 = db.start_fn(expr=[db.put_node(x) for x in expr], cache=True)  # we should have a cached result now
            assert any(re.match(r'.*using cached dag: CachedFnDag.*', o) for o in cm.output)
            assert n1 is not None
            assert n1().value().value == 'zomg'

    def test_walk(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        def walk_types(obj):
            return Counter(x.type for x in db.walk(obj))
        with db.tx(True):
            db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum({'foo': 42})))
            db.commit(n0)
            assert walk_types(self.get_dag(db, 'd0')) == {'dag': 1, 'datum': 2, 'node': 1}
            assert walk_types(self.get_dag(db, 'd0')().result) == {'datum': 2, 'node': 1}
            assert walk_types(self.get_dag(db, 'd0')().result().value) == {'datum': 2}

    def test_walk_ordered(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        def walk_types(obj, ordered=False):
            f = db.walk_ordered if ordered else db.walk
            return Counter(x.type for x in f(obj))
        with db.tx(True):
            db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum({'foo': 42})))
            db.commit(n0)
            assert walk_types(self.get_dag(db, 'd0')) == {'dag': 1, 'datum': 2, 'node': 1}
            assert walk_types(self.get_dag(db, 'd0')().result) == {'datum': 2, 'node': 1}
            assert walk_types(self.get_dag(db, 'd0')().result().value) == {'datum': 2}
            assert db.walk(self.get_dag(db, 'd0')) == set(db.walk_ordered(self.get_dag(db, 'd0')))
            self.assertCountEqual(
                list(db.walk(self.get_dag(db, 'd0'))),
                db.walk_ordered(self.get_dag(db, 'd0')),
            )

    def test_dump_load(self):
        d0 = Repo(self.tmpdir, 'testy@test', create=True)
        with d0.tx(True):
            d0.begin(name='d0', message='1st dag')
            n0 = d0.put_node(Literal(d0.put_datum({'foo': 42})))
            dump = d0.dump(n0)
        with TemporaryDirectory() as tmpd:
            d1 = Repo(tmpd, 'testy@test', create=True)
            with d1.tx(True):
                n1 = d1.load(dump)
                assert unroll_datum(n1().value) == {'foo': 42}

    def test_resource_access(self):
        data = {'a': 2, 'b': {1, 2, 3}}
        resource = Resource(data)
        assert resource.data == data

    def test_datatypes(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            db.begin(name='d0', message='1st dag')
            data = {
                'int': 23,
                'float': 12.43,
                'bool': True,
                'null': None,
                'string': 'qwer',
                'list': [3, 4, 5],
                'map': {'a': 2, 'b': 'asdf'},
                'set': {12, 13, 'a', 3.4},
                'resource': Resource({'a': 1, 'b': 2}),
                'composite': {'asdf': {2, Resource({'a': 8, 'b': 2})}}
            }
            for k, v in data.items():
                assert from_json(to_json(v)) == v
                ref = db.put_node(Literal(db.put_datum(v)))
                assert isinstance(ref, Ref)
                node = ref()
                assert isinstance(node, Node)
                val = unroll_datum(node.value())
                assert val == v, f'failed {k}'
