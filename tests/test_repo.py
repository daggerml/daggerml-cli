#!/usr/bin/env python3
import unittest
from collections import Counter
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
from typing import Any

from tabulate import tabulate

from daggerml_cli.repo import (
    Ctx,
    Datum,
    Error,
    Expr,
    FnDag,
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



@dataclass
class FnStart:
    repo: Any
    expr: Any
    create: bool = False
    fndb: Repo = field(init=False)
    fndag: Ref = field(init=False)
    fnidx: Ref = field(init=False)
    tmpd: str = field(init=False)
    waiter: Ref = field(init=False)
    dump: str = field(init=False)

    def __post_init__(self):
        self.waiter = self.repo.start_fn(
            expr=self.expr
        )
        self.dump = self.waiter().dump
        if self.create:
            self._tmpd = TemporaryDirectory()
            self.tmpd = self._tmpd.__enter__()
            self.fndb = Repo(self.tmpd, create=True)
            with self.fndb.tx(True):
                self.fndag = self.fndb.load_ref(self.dump)
                self.fnidx = self.fndb.begin(name='fndag', message='executing', dag=self.fndag)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if hasattr(self, '_tmpd'):
            self._tmpd.__exit__(*args, **kwargs)


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
            index = db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum(Resource('a'))), index=index)
            db.commit(n0, index=index)

    def test_fndag_id(self):
        expr = [Resource(self.id()), ['howdy', 1], 2]
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            expr_datum = db.put_datum([x for x in expr])
            expr_node = db(Node(Expr(expr_datum)))
            fndag = db(FnDag(set([expr_node]), None, None, expr_node), return_on_error=True)
            dag0_id = fndag.to
        # same expr => same ID
        with TemporaryDirectory() as tmpd:
            db = Repo(tmpd, 'testy@test', create=True)
            with db.tx(True):
                expr_datum = db.put_datum([x for x in expr])
                expr_node = db(Node(Expr(expr_datum)))
                fndag = db(FnDag(set([expr_node]), None, None, expr_node), return_on_error=True)
                dag1_id = fndag.to
        assert dag0_id == dag1_id
        # different expr => different ID
        with TemporaryDirectory() as tmpd:
            db = Repo(tmpd, 'testy@test', create=True)
            with db.tx(True):
                expr_datum = db.put_datum([x for x in [*expr, 4]])
                expr_node = db(Node(Expr(expr_datum)))
                fndag = db(FnDag(set([expr_node]), None, None, expr_node), return_on_error=True)
                dag1_id = fndag.to
        assert dag0_id != dag1_id

    def test_dag_id(self):
        expr = [Resource(self.id()), ['howdy', 1], 2]
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index = db.begin(name='d0', message='1st dag')
            _expr = [db.put_node(Literal(db.put_datum(x)), index=index) for x in expr]
            with FnStart(db, _expr, create=True) as fns:
                fnid = fns.fndag.to
        with TemporaryDirectory() as tmpd:
            db = Repo(tmpd, 'testy@test', create=True)
            with db.tx(True):
                index = db.begin(name='d0', message='1st dag')
                _expr = [db.put_node(Literal(db.put_datum(x)), index=index) for x in expr]
                with FnStart(db, _expr, create=True) as fns:
                    fnid2 = fns.fndag.to
        assert fnid == fnid2

    def test_cache_newdag(self):
        expr = [Resource(self.id()), ['howdy', 1], 2]
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index = db.begin(name='d0', message='1st dag')
            _expr = [db.put_node(Literal(db.put_datum(x)), index=index) for x in expr]
            # start a cached run
            fns = FnStart(db, _expr, create=True)
        with fns, fns.fndb.tx(True):
            node = fns.fndb.put_node(
                Literal(fns.fndb.put_datum('zomg')),
                index=fns.fnidx
            )
            node = fns.fndb.commit(node, fns.fnidx)
            dump = fns.fndb.dump_ref(fns.fndag)
        with db.tx(True):
            db.load_ref(dump)
            result = db.get_fn_result(index, fns.waiter)
            db.commit(result, index)

            # new result should be cached
            index = db.begin(name='d1', message='2nd dag')
            _expr = [db.put_node(Literal(db.put_datum(x)), index=index) for x in expr]
            waiter = db.start_fn(expr=_expr)
            node = db.get_fn_result(index, waiter)
            assert isinstance(node, Ref)
            assert node.type == 'node'
            assert node().value().value == 'zomg'
            # different expr should not
            _expr = [db.put_node(Literal(db.put_datum(x)), index=index) for x in [*expr, 5]]
            waiter = db.start_fn(expr=_expr)
            assert db.get_fn_result(index, waiter) is None

    def test_get_nodeval(self):
        data = {'asdf': 23}
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index = db.begin(name='d0', message='1st dag')
            node = db.put_node(Literal(db.put_datum(data)), index=index)
            res = db.get_node_value(node)
            assert res == data

    def test_walk(self):
        db = Repo(self.tmpdir, 'testy@test', create=True)
        def walk_types(obj):
            return Counter(x.type for x in db.walk(obj))
        with db.tx(True):
            index = db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum({'foo': 42})), index=index)
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
            index = db.begin(name='d0', message='1st dag')
            n0 = db.put_node(Literal(db.put_datum({'foo': 42})), index=index)
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
            index = db.begin(name='d0', message='1st dag')
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
                ref = db.put_node(Literal(db.put_datum(v)), index=index)
                assert isinstance(ref, Ref)
                node = ref()
                assert isinstance(node, Node)
                val = unroll_datum(node.value())
                assert val == v, f'failed {k}'

    def test_dag_dump_n_load(self):
        rsrc = Resource('asdf')
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            index = db.begin(name='d0', message='1st dag')
            db.put_node(Literal(db.put_datum(rsrc)), index=index)
            dag = index().dag
            dump = db.dump_ref(dag)
        with TemporaryDirectory() as tmpd:
            repo = Repo(tmpd, create=True)
            with repo.tx(True):
                ref = repo.load_ref(dump)
                dump2 = repo.dump_ref(ref)
        assert ref == dag
        assert dump == dump2
