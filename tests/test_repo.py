import unittest
from daggerml_cli.repo import Repo, Resource
from pprint import pp
from tabulate import tabulate
from tempfile import TemporaryDirectory


def tab(rows):
    print(tabulate(rows, tablefmt='fancy_grid'))


class TestRepo(unittest.TestCase):

    def setUp(self):
        self.tmpdir_ctx = TemporaryDirectory()
        self.tmpdir = self.tmpdir_ctx.__enter__()

    def tearDown(self):
        self.tmpdir_ctx.__exit__(None, None, None)
        self.tmpdir_ctx = self.tmpdir = None

    def test_create_dag(self):
        print()

        db = Repo(self.tmpdir)

        db.begin('d0')
        n0 = db.put_literal_node([1, 2, 3])
        n1 = db.put_literal_node({'foo': 42, 'bar': ['baz', 'baf']})
        n2 = db.put_literal_node(Resource({'a': 1}))
        db.commit(n2)
        assert n0 and n1

        db.begin('d1').commit(db.put_literal_node(88))
        db.begin('d0').commit(db.put_literal_node('asdf'))

        db.checkout('foop', create=True)
        db.begin('d0').commit(db.put_literal_node(99))

        db.begin('d2')
        db.put_literal_node(['x', 'y', 'z'])
        pp(db.dump('repo'))
        db.commit(db.put_literal_node(47))

        db.gc()

        print()
        tab(db.dump('db'))
