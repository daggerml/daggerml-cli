import unittest
from pprint import pp
from tempfile import TemporaryDirectory

import pytest
from tabulate import tabulate

from daggerml_cli.repo import Literal, Load, Ref, Repo, Resource, unroll_datum


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
            # state changes here, parent_dag is set, subsequent put_node and commit
            # are to the parent_dag not the d1 dag
            db.begin(expr=expr)
            n0 = db.put_node(Literal(db.put_datum('omg')))
            # state changes again, parent_dag is unset and Fn node in d1 is returned
            n1 = db.commit(n0, cache=True)
            db.commit(n1)

            assert self.dag_result(self.get_dag(db, 'd1')) == 'omg'

            db.begin(name='d2', message='3rd dag')
            db.begin(expr=expr)

            assert self.dag_result(db.cached_dag) == 'omg'

            n0 = db.put_node(Literal(db.put_datum('hello world')))
            with pytest.raises(AssertionError):
                db.commit(n0, cache=True)
            n1 = db.commit(n0, cache=db.cached_dag)
            db.commit(n1)

            print()
            db.gc()
            dump(db)
