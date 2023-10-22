import unittest
import daggerml_cli.repo
import daggerml_cli.api as api
from daggerml_cli.repo import Repo, Resource, Ref, Node, Literal, Load, Fnex, from_data, to_data
from pprint import pp
from tabulate import tabulate
from tempfile import TemporaryDirectory
from daggerml_cli.pack import EXT_CODE


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
        db = Repo(self.tmpdir, 'testy@test', create=True)
        with db.tx(True):
            db.begin('d0', 'first dag')
            db.commit(db.put_node(Node(Literal(db.put_datum(Resource({'foo': 42}))))))

            db.begin('d1', '2nd dag')
            expr = []
            expr.append(db.put_node(Node(Load(db.get_dag('d0')))))
            expr.append(db.put_node(Node(Literal(db.put_datum(['Fnapp', 1])))))
            expr.append(db.put_node(Node(Literal(db.put_datum(2)))))
            f0 = db.put_fn(expr)
            f1 = db.put_fn(expr, {'info': 100}, replace=f0)
            with self.assertRaisesRegex(AssertionError, f'fnex is older than {f1.fnex.to}'):
                f2 = db.put_fn(expr, {'info': 200}, db.put_datum(444), replace=f0)
            f2 = db.put_fn(expr, {'info': 200}, db.put_datum(444), replace=f1)
            db.commit(Node(f2))

            print()
            db.gc()
            dump(db)

            pp(to_data(f1))
            pp(from_data(to_data(f1)))

            data = Resource({'foo': [1, 1.5], 'bar': {'zomg', True, None}, 'baz': {'baf': 'qux'}})
            pp(to_data(data))
            pp(from_data(to_data(data)))

        # print()
        # api.commit_log_graph(db=db)
