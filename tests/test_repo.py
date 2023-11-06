import unittest
from pprint import pp
from tempfile import TemporaryDirectory

import pytest
from tabulate import tabulate

from daggerml_cli.repo import CachedFnDag, Literal, Load, Repo, Resource, unroll_datum


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
            assert self.dag_result(self.get_dag(db, 'd0')) == Resource({'foo': 42})

            db.begin(name='d1', message='2nd dag')
            expr = [
                db.put_node(Load(db.ctx(db.index).dags['d0'])),
                db.put_node(Literal(db.put_datum(['howdy', 1]))),
                db.put_node(Literal(db.put_datum(2))),
            ]
            db.begin(expr=expr)  # fn application, ops on db are now applied to the fndag
            assert db.cached_dag is None
            n0 = db.put_node(Literal(db.put_datum('omg')))  # add node to fndag
            n1 = db.commit(n0, cache=True)  # commit fndag, cache it, and return Fn node
            db.commit(n1)  # commit d1 dag with the Fn node as its result
            assert self.dag_result(self.get_dag(db, 'd1')) == 'omg'

            db.begin(name='d2', message='3rd dag')
            db.begin(expr=expr)  # we should have a cached result now
            assert isinstance(db.cached_dag(), CachedFnDag)
            assert self.dag_result(db.cached_dag) == 'omg'
            n0 = db.put_node(Literal(db.put_datum('hello world')))  # not using cached result
            with pytest.raises(AssertionError):
                db.commit(n0, cache=True)  # can't commit a new cache value without compare and swap
            n1 = db.commit(n0, cache=db.cached_dag)  # works with compare and swap
            db.commit(n1)

            db.begin(name='d3', message='4rd dag')
            db.begin(expr=expr)
            assert isinstance(db.cached_dag(), CachedFnDag)
            assert self.dag_result(db.cached_dag) == 'hello world'
            n2 = db.commit()  # empty commit means use cached value
            db.commit(n2)

            assert self.dag_result(self.get_dag(db, 'd3')) == 'hello world'

            # return

            print()
            db.gc()
            dump(db)
