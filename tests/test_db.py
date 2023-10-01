import unittest
from tempfile import TemporaryDirectory
from daggerml_cli.db import Db, DEFAULT
from daggerml_cli.util import unpackb


class TestDb(unittest.TestCase):

    def setUp(self):
        self.tmpdir_ctx = TemporaryDirectory()
        self.tmpdir = self.tmpdir_ctx.__enter__()

    def tearDown(self):
        self.tmpdir_ctx.__exit__(None, None, None)
        self.tmpdir_ctx = self.tmpdir = None

    def dump(self, db):
        print('\n=============== DB DUMP ===============')
        with db.tx() as tx:
            for (k, v) in iter(tx.cursor()):
                print(f'{k.decode()}\t\t{unpackb(v)}')
        print('=============== DB DUMP ===============')

    def print_log(self, db, branch=None, dag=None):
        dag_at = f'{dag}@' if dag is not None else ''
        print(f'{dag_at}{branch or db.head}:\t{db.log(branch, dag)}')

    def test_create_dag(self):
        print()

        db = Db(self.tmpdir)

        self.print_log(db)

        db.begin('d0')
        n0 = db.put_literal_node([1, 2, 3])
        n1 = db.put_literal_node({'foo': 42, 'bar': ['baz', 'baf']})
        db.commit(n1)
        assert n0 and n1

        self.print_log(db)

        db.begin('d1').commit(db.put_literal_node(88))
        db.begin('d0').commit(db.put_literal_node('asdf'))

        self.print_log(db)

        db.checkout('foop', create=True)
        db.begin('d0').commit(db.put_literal_node(99))

        self.print_log(db)
        self.print_log(db, DEFAULT)

        self.print_log(db, DEFAULT, 'd0')
        self.print_log(db, 'foop', 'd0')

        self.print_log(db, DEFAULT, 'd1')
        self.print_log(db, 'foop', 'd1')

        self.dump(db)