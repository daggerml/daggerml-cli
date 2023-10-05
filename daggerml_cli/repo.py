from daggerml_cli.util import packb, unpackb, packb64, unpackb64, sort_dict, dbenv, now
from hashlib import md5
from uuid import uuid4


DEFAULT = 'main'


class Repo:

    @classmethod
    def new(cls, b64state):
        return cls(*unpackb64(b64state))

    def __init__(self, path, branch=DEFAULT, index=None, dag=None):
        self.env, self.db = dbenv(path)
        self.path = path
        self.head = branch
        self.index = index
        self.dag = dag

    def state(self):
        return packb64([self.path, self.head, self.index, self.dag])

    def tx(self, write=False):
        return self.env.begin(write=write, buffers=True)

    def dump_db(self, tx):
        rows = []
        for type, db in self.db.items():
            for (k, v) in iter(tx.cursor(db=db)):
                rows.append([type, bytes(k).decode(), unpackb(v)])
        return rows

    def empty_dump_result(self):
        return {k: {} for k in self.db.keys()}

    def dump_repo(self, tx, result):
        for db in ['index', 'head']:
            for (k, v) in iter(tx.cursor(db=self.db[db])):
                self.dump_branch(tx, bytes(k).decode(), result, (db == 'index'))
        return result

    def exists_branch(self, tx, name, index=False):
        type = 'index' if index else 'head'
        return self.exists_obj(tx, type, name)

    def get_branch(self, tx, name, index=False):
        type = 'index' if index else 'head'
        return self.get_obj(tx, type, name)

    def put_branch(self, tx, name, commit_key, index=False):
        type = 'index' if index else 'head'
        tx.put(name.encode(), packb(commit_key), db=self.db[type])

    def del_branch(self, tx, name, index=False):
        type = 'index' if index else 'head'
        tx.delete(name.encode(), db=self.db[type])

    def dump_branch(self, tx, branch_key, result, index=False):
        db = 'index' if index else 'head'
        commit_key = result[db][branch_key] = self.get_branch(tx, branch_key, index=(index))
        self.dump_commit(tx, commit_key, result)
        return result

    def exists_index(self, tx, name):
        return self.exists_branch(tx, name, index=True)

    def get_index(self, tx, name):
        return self.get_branch(tx, name, index=True)

    def put_index(self, tx, name, commit_key):
        self.put_branch(tx, name, commit_key, index=True)

    def del_index(self, tx, name):
        return self.del_branch(tx, name, index=True)

    def exists_obj(self, tx, type, key):
        if key is not None:
            return tx.get(key.encode(), db=self.db[type]) is not None

    def get_obj(self, tx, type, key):
        if key is not None:
            return unpackb(tx.get(key.encode(), db=self.db[type]))

    def put_obj(self, tx, type, obj):
        data = packb(obj)
        hash = md5(data).hexdigest()
        keyb = hash.encode()
        if tx.get(keyb, db=self.db[type]) is None:
            tx.put(keyb, data, db=self.db[type])
        return hash

    def get_tree(self, tx, key):
        return self.get_obj(tx, 'tree', key) or {}

    def put_tree(self, tx, tree):
        return self.put_obj(tx, 'tree', sort_dict(tree))

    def dump_tree(self, tx, tree_key, result):
        tree = result['tree'][tree_key] = self.get_tree(tx, tree_key)
        [self.dump_dag(tx, v, result) for v in tree.values()]
        return result

    def get_commit(self, tx, key):
        return self.get_obj(tx, 'commit', key) or [None, None, None]

    def put_commit(self, tx, parent_key, tree_key):
        return self.put_obj(tx, 'commit', [parent_key, tree_key, now()])

    def dump_commit(self, tx, commit_key, result):
        commit = result['commit'][commit_key] = self.get_commit(tx, commit_key)
        self.dump_tree(tx, commit[1], result)
        self.dump_commit(tx, commit[0], result) if commit[0] else None
        return result

    def get_datum(self, tx, key):
        return self.get_obj(tx, 'datum', key)

    def put_datum(self, tx, value):
        t = type(value)
        if t in [type(None), str, bool, int, float]:
            data = value
        elif t in [list, tuple]:
            data = [self.put_datum(tx, x) for x in value]
        elif t in [dict]:
            data = sort_dict({k: self.put_datum(tx, v) for k, v in value.items()})
        else:
            raise ValueError(f'unknown type: {t}')
        return self.put_obj(tx, 'datum', data)

    def dump_datum(self, tx, datum_key, result):
        result['datum'][datum_key] = self.get_datum(tx, datum_key)
        return result

    def get_dag(self, tx, key):
        return self.get_obj(tx, 'dag', key) or [[], None]

    def put_dag(self, tx, node_keys, result_node_key):
        return self.put_obj(tx, 'dag', [sorted(node_keys), result_node_key])

    def dump_dag(self, tx, dag_key, result):
        dag = result['dag'][dag_key] = self.get_dag(tx, dag_key)
        for n in filter(lambda x: x, [*dag[0], dag[1]]):
            self.dump_node(tx, n, result)
        return result

    def get_node(self, tx, key):
        return self.get_obj(tx, 'node', key)

    def put_node(self, tx, dag_name, type, data):
        commit_key = self.get_index(tx, self.index)
        parent_commit_key, tree_key, *_ = self.get_commit(tx, commit_key)
        tree = self.get_tree(tx, tree_key)
        nodes, _ = self.get_dag(tx, tree[dag_name])
        node_key = self.put_obj(tx, 'node', [type, data])
        nodes.append(node_key)
        new_dag_key = self.put_dag(tx, nodes, None)
        tree[dag_name] = new_dag_key
        new_tree_key = self.put_tree(tx, tree)
        index_commit_key = self.put_commit(tx, parent_commit_key, new_tree_key)
        self.put_index(tx, self.index, index_commit_key)
        return node_key

    def dump_node(self, tx, node_key, result):
        node = result['node'][node_key] = self.get_node(tx, node_key)
        self.dump_datum(tx, node[1], result)
        return result

    ############################################################################
    # PUBLIC API ###############################################################
    ############################################################################

    def dump(self, what):
        with self.tx() as tx:
            if what == 'repo':
                return self.dump_repo(tx, self.empty_dump_result())
            elif what == 'db':
                return self.dump_db(tx)

    def gc(self):
        with self.tx(True) as tx:
            dump = self.dump_repo(tx, self.empty_dump_result())
            for db in dump.keys():
                for (k, v) in iter(tx.cursor(db=self.db[db])):
                    if not bytes(k).decode() in dump[db]:
                        tx.delete(k, db=self.db[db])

    def checkout(self, branch=DEFAULT, create=False):
        assert self.index is None, 'can not switch branches with a dirty index'
        if create:
            with self.tx(True) as tx:
                assert not self.exists_branch(tx, branch), 'can not create a branch which exists'
                self.put_branch(tx, branch, self.get_branch(tx, self.head))
        with self.tx() as tx:
            assert self.exists_branch(tx, branch), 'branch not found'
        self.head = branch

    def begin(self, dag_name):
        index_name = uuid4().hex
        with self.tx(True) as tx:
            head_commit_key = self.get_branch(tx, self.head)
            _, tree_key, *_ = self.get_commit(tx, head_commit_key)
            tree = self.get_tree(tx, tree_key)
            tree[dag_name] = None
            new_tree_key = self.put_tree(tx, tree)
            index_commit_key = self.put_commit(tx, head_commit_key, new_tree_key)
            self.put_index(tx, index_name, index_commit_key)
        self.dag = dag_name
        self.index = index_name
        return self

    def commit(self, result_node_key):
        with self.tx(True) as tx:
            commit_key = self.get_index(tx, self.index)
            parent_commit_key, tree_key, *_ = self.get_commit(tx, commit_key)
            tree = self.get_tree(tx, tree_key)
            nodes, _ = self.get_dag(tx, tree[self.dag])
            new_dag_key = self.put_dag(tx, nodes, result_node_key)
            head_commit_key = self.get_branch(tx, self.head)
            head_parent_key, head_tree_key, *_ = self.get_commit(tx, head_commit_key)
            head_tree = self.get_tree(tx, head_tree_key)
            head_tree[self.dag] = new_dag_key
            new_tree_key = self.put_tree(tx, head_tree)
            new_commit_key = self.put_commit(tx, head_commit_key, new_tree_key)
            self.put_branch(tx, self.head, new_commit_key)
            self.del_index(tx, self.index)
        self.dag = None
        self.index = None
        return self

    def put_literal_node(self, value):
        with self.tx(True) as tx:
            datum_key = self.put_datum(tx, value)
            return self.put_node(tx, self.dag, 'literal', datum_key)

    def put_load_node(self, commit, dag):
        pass
