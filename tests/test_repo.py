import unittest
from daggerml_cli.repo import Repo, Resource, Ref, Head
from pprint import pp
from tabulate import tabulate
from tempfile import TemporaryDirectory


def dump(repo, count=None):
    rows = []
    for db in repo.dbs.keys():
        for (k, v) in repo.cursor(db):
            k = bytes(k).decode()
            rows.append([len(rows) + 1, k, repo.get(Ref(k))])
    rows = rows[:min(count, len(rows))] if count is not None else rows
    print(f'\n{tabulate(rows, tablefmt="simple_grid")}')


class TestRepo(unittest.TestCase):

    def setUp(self):
        self.tmpdir_ctx = TemporaryDirectory()
        self.tmpdir = self.tmpdir_ctx.__enter__()

    def tearDown(self):
        self.tmpdir_ctx.__exit__(None, None, None)
        self.tmpdir_ctx = self.tmpdir = None

    def test_create_dag(self):
        db = Repo(self.tmpdir)
        with db.tx(True):
            db.begin('d0', meta={'foop': 'barp'})
            x0 = db.put_datum({'foo': ['bar', [1, 2, 3]]})
            n0 = db.put_node('literal', x0, meta=x0)
            db.commit(n0)

            db.create_branch(Ref('head/foop'), db.head)
            db.checkout(Ref('head/foop'))

            db.begin('d1')
            db.commit(db.put_node('literal', db.put_datum(75)))

            db.begin('d2')
            db.commit(db.put_node('literal', db.put_datum(99)))

            db.checkout(Ref('head/main'))

            db.begin('d3')
            db.commit(db.put_node('literal', db.put_datum('d3')))

            db.begin('d0')
            db.commit(db.put_node('literal', db.put_datum('d0')))

            db.begin('d1')
            db.commit(db.put_node('literal', db.put_datum('d1')))

            a = Ref('head/main')().commit
            b = Ref('head/foop')().commit
            m0 = db.merge(a, b)

            print()
            pp([db.head, a, b, m0])

            db.checkout(db.set_head(db.head, Head(m0)))

            db.gc()
            dump(db, 10)
