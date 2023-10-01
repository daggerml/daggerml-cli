from daggerml_cli.util import packb, unpackb, packb64, unpackb64, sort_dict, dbenv
from hashlib import md5
from uuid import uuid4


DEFAULT = 'main'


class Db:

    @classmethod
    def new(cls, b64state):
        return cls(**unpackb64(b64state))

    def __init__(self, path, branch=DEFAULT, index=None, dag=None):
        self.env = dbenv(path)
        self.path = path
        self.head = branch
        self.index = index
        self.dag = dag

    def state(self):
        return packb64({
            'path': self.path,
            'branch': self.head,
            'index': self.index,
            'dag': self.dag,
        })

    def tx(self, write=False):
        return self.env.begin(write=write)

    def dump(self):
        rows = []
        with self.tx() as tx:
            for (k, v) in iter(tx.cursor()):
                rows.append([k.decode(), unpackb(v)])
        return rows

    def exists_branch(self, tx, name, index=False):
        type = 'index' if index else 'branch'
        return self.exists_obj(tx, f'{type}/{name}')

    def get_branch(self, tx, name, index=False):
        type = 'index' if index else 'branch'
        return self.get_obj(tx, f'{type}/{name}')

    def put_branch(self, tx, name, commit_key, index=False):
        type = 'index' if index else 'branch'
        key = f'{type}/{name}'
        tx.put(key.encode(), packb(commit_key))
        return key

    def del_branch(self, tx, name, index=False):
        type = 'index' if index else 'branch'
        key = f'{type}/{name}'
        tx.delete(key.encode())

    def exists_index(self, tx, name):
        return self.exists_branch(tx, name, index=True)

    def get_index(self, tx, name):
        return self.get_branch(tx, name, index=True)

    def put_index(self, tx, name, commit_key):
        return self.put_branch(tx, name, commit_key, index=True)

    def del_index(self, tx, name):
        return self.del_branch(tx, name, index=True)

    def exists_obj(self, tx, key):
        if key is not None:
            return tx.get(key.encode()) is not None

    def get_obj(self, tx, key):
        if key is not None:
            return unpackb(tx.get(key.encode()))

    def put_obj(self, tx, type, obj):
        data = packb(obj)
        hash = md5(data).hexdigest()
        key = f'{type}/{hash}'
        keyb = key.encode()
        if tx.get(keyb) is None:
            tx.put(keyb, data)
        return key

    def get_dags(self, tx, key):
        return self.get_obj(tx, key) or {}

    def put_dags(self, tx, dags):
        return self.put_obj(tx, 'dags', sort_dict(dags))

    def get_commit(self, tx, key):
        return self.get_obj(tx, key) or [None, None]

    def put_commit(self, tx, parent_key, dags_key):
        return self.put_obj(tx, 'commit', [parent_key, dags_key])

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

    def get_dag(self, tx, key):
        return self.get_obj(tx, key) or [[], None]

    def put_dag(self, tx, node_keys, result_node_key):
        return self.put_obj(tx, 'dag', [sorted(node_keys), result_node_key])

    def put_node(self, tx, dag_name, type, data):
        commit_key = self.get_index(tx, self.index)
        parent_commit_key, dags_key = self.get_commit(tx, commit_key)
        dags = self.get_dags(tx, dags_key)
        nodes, _ = self.get_dag(tx, dags[dag_name])
        node_key = self.put_obj(tx, 'node', [type, data])
        nodes.append(node_key)
        new_dag_key = self.put_dag(tx, nodes, None)
        dags[dag_name] = new_dag_key
        new_dags_key = self.put_dags(tx, dags)
        index_commit_key = self.put_commit(tx, parent_commit_key, new_dags_key)
        self.put_index(tx, self.index, index_commit_key)
        return node_key

    ############################################################################
    # PUBLIC API ###############################################################
    ############################################################################

    def log(self, branch=None, dag=None):
        commits = []
        with self.tx() as tx:
            commit_key = self.get_branch(tx, branch or self.head)
            while commit_key:
                ck = commit_key
                commit_key, dags_key = self.get_commit(tx, commit_key)
                if dag:
                    dag_key = self.get_dags(tx, dags_key).get(dag)
                    if dag_key:
                        if len(commits) and dag_key == commits[-1][1]:
                            commits.pop()
                        commits.append([ck, dag_key])
                else:
                    commits.append(ck)
        return commits

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
            _, dags_key = self.get_commit(tx, head_commit_key)
            dags = self.get_dags(tx, dags_key)
            dags[dag_name] = None
            new_dags_key = self.put_dags(tx, dags)
            index_commit_key = self.put_commit(tx, head_commit_key, new_dags_key)
            self.put_index(tx, index_name, index_commit_key)
        self.dag = dag_name
        self.index = index_name
        return self

    def commit(self, result_node_key):
        with self.tx(True) as tx:
            commit_key = self.get_index(tx, self.index)
            parent_commit_key, dags_key = self.get_commit(tx, commit_key)
            dags = self.get_dags(tx, dags_key)
            nodes, _ = self.get_dag(tx, dags[self.dag])
            new_dag_key = self.put_dag(tx, nodes, result_node_key)
            head_commit_key = self.get_branch(tx, self.head)
            head_parent_key, head_dags_key = self.get_commit(tx, head_commit_key)
            head_dags = self.get_dags(tx, head_dags_key)
            head_dags[self.dag] = new_dag_key
            new_dags_key = self.put_dags(tx, head_dags)
            new_commit_key = self.put_commit(tx, head_commit_key, new_dags_key)
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
