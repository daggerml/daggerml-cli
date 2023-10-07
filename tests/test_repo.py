import unittest
from daggerml_cli.pack import unpackb
from daggerml_cli.repo import Repo, Resource, Meta
from pprint import pp
from tabulate import tabulate
from tempfile import TemporaryDirectory


def dump(repo, rows=[]):
    with repo.tx():
        for db in repo.db.keys():
            for (k, v) in repo.cursor(db):
                k = bytes(k).decode()
                rows.append([len(rows) + 1, db, k, repo(k)])
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
        m0 = Meta(db.put_node('literal', db.put_datum({"foo": ["bar", "baz"]})))
        db.commit(db.put_node('literal', db.put_datum(Resource({'hello': 'world'}))), meta=m0)

        db = Repo(self.tmpdir)
        db.begin('d0')
        db.commit(db.put_node('literal', db.put_datum(75)))

        db.gc()
        dump(db)

        # with db.tx():
        #     pp(db.walk('dag/60e558d76bcbaaf82e347d04d41636d6'))
