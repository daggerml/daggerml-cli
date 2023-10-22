import unittest
import daggerml_cli.api as api
from daggerml_cli.repo import Repo, Resource, Ref, Node, Literal, Load
from pprint import pp
from tabulate import tabulate
from tempfile import TemporaryDirectory


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
        self.tmpdir_ctx = self.tmpdir = None

    def test_create_dag(self):
        print('---')
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            db.begin('d0', 'first dag')
            db.commit(db.put_node(Node(Literal(db.put_datum(Resource({'foo': 42}))))))

            db.begin('d1', 'second dag')
            db.commit(db.put_node(Node(Load(db.get_dag('d0')))))

            # db.begin('d2', '3rd dag')
            # db.put_node(Node(Fn(

            print()
            db.gc()
            dump(db)

        # print()
        # api.commit_log_graph(db=db)
