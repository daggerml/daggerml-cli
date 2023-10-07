import unittest
from daggerml_cli.pack import unpackb
from daggerml_cli.repo import Repo, Resource
from pprint import pp
from tabulate import tabulate
from tempfile import TemporaryDirectory


def dump(repo, rows=[]):
    with repo.tx():
        for db in repo.db.keys():
            for (k, v) in repo.cursor(db):
                rows.append([len(rows) + 1, db, bytes(k).decode(), unpackb(v)])
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
        db.begin('d0')
        db = Repo.new(db.state)
        db.put_node('literal', db.put_datum([1, 2, 3]))
        db.put_node('literal', db.put_datum([1, 2, 3]))
        db.commit(db.put_node('literal', db.put_datum(Resource({'hello': 'world'}))))
        db = Repo.new(db.state)

        db.gc()
        dump(db)
