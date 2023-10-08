import unittest
from daggerml_cli.repo import Repo, Resource, Ref
from pprint import pp
from tabulate import tabulate
from tempfile import TemporaryDirectory


def dump(repo, rows=None):
    rows = [] if rows is None else rows
    with repo.tx():
        for db in repo.dbs.keys():
            for (k, v) in repo.cursor(db):
                k = bytes(k).decode()
                rows.append([len(rows) + 1, k, repo.get(Ref(k))])
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
        db.begin('d0', meta={'foop': 'barp'})
        x0 = db.put_datum({'foo': ['bar', [1, 2, 3]]})
        n0 = db.put_node('literal', x0, meta=x0)
        db.commit(n0)

        db.create_branch(Ref('head/foop'))
        db.checkout(Ref('head/foop'))

        db.begin('d1')
        db.commit(db.put_node('literal', db.put_datum(75)))

        db.begin('d2')
        db.commit(db.put_node('literal', db.put_datum(99)))

        db.gc()
        dump(db)

        db = Repo(self.tmpdir, head=Ref('head/main'))

        # with db.tx():
        #     pp(db.walk('dag/60e558d76bcbaaf82e347d04d41636d6'))
