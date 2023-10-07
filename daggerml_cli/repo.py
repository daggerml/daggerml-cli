from daggerml_cli.db import dbenv
from daggerml_cli.pack import packb, unpackb, packb64, unpackb64, packb_type
from daggerml_cli.util import now
from dataclasses import dataclass, fields
from hashlib import md5
from uuid import uuid4


DEFAULT = 'head/main'


@packb_type
class Resource:
    data: dict


@packb_type
class Datum:
    value: type(None) | str | bool | int | float | Resource | list | dict | set


@packb_type
class Node:
    type: str
    value: str | None


@packb_type
class Dag:
    nodes: set
    result: str = None
    error: dict = None


@packb_type
class Tree:
    dags: dict


@packb_type
class Commit:
    parent: str = None
    tree: str = None
    timestamp: str = now()


@packb_type
class Head:
    commit: str


@packb_type
class Index:
    commit: str


@dataclass
class Repo:
    path: str
    head: str = DEFAULT
    index: str = None
    dag: str = None

    def __post_init__(self):
        self.env, self.db = dbenv(self.path)
        self._tx = None

    def __call__(self, key, obj=None):
        if isinstance(key, str) and obj is None:
            db = key.split('/')[0]
            result = self._tx.get(key.encode(), db=self.db[db])
            return unpackb(result) if result else None
        key, obj = (key, obj) if obj else (obj, key)
        if obj is not None:
            db = obj.__class__.__name__.lower()
            data = packb(obj)
            key2 = key or f'{db}/{md5(data).hexdigest()}'
            comp = None
            if key is None:
                comp = self._tx.get(key2.encode(), db=self.db[db])
                assert comp is None or comp == data
            if key is None or comp is None:
                self._tx.put(key2.encode(), data, db=self.db[db])
            return key2

    def delete(self, key):
        db = key.split('/')[0]
        self._tx.delete(key.encode(), db=self.db[db])

    def tx(self, write=False):
        tx = self._tx = self.env.begin(write=write, buffers=True)
        return tx

    def cursor(self, db):
        return iter(self._tx.cursor(db=self.db[db]))

    def walk(self, key, result=set()):
        if key:
            type = key.split('/')[0]
            d = self(key)
            if type == 'datum':
                if isinstance(d.value, (list, set)):
                    [self.walk(k, result) for k in d.value]
                elif isinstance(d.value, dict):
                    [self.walk(k, result) for k in d.value.values()]
            elif type == 'node':
                self.walk(d.value, result)
            elif type == 'dag':
                [self.walk(k, result) for k in d.nodes.union({d.result})]
            elif type == 'tree':
                [self.walk(k, result) for k in d.dags.values()]
            elif type == 'commit':
                [self.walk(k, result) for k in [d.parent, d.tree]]
            elif type in ['head', 'index']:
                self.walk(d.commit, result)
            result.add(key)
        return result

    ############################################################################
    # PUBLIC API ###############################################################
    ############################################################################

    @classmethod
    def new(cls, b64state):
        return cls(*unpackb64(b64state))

    @property
    def state(self):
        return packb64([getattr(self, x.name) for x in fields(self)])

    def gc(self):
        with self.tx(True):
            live_objs = set()
            for db in ['head', 'index']:
                for (k, _) in self.cursor(db):
                    self.walk(bytes(k).decode(), live_objs)
            for db in self.db.keys():
                for (k, _) in self.cursor(db):
                    k = bytes(k).decode()
                    self.delete(k) if k not in live_objs else None

    def begin(self, dag):
        index = f'index/{uuid4().hex}'
        with self.tx(True):
            head = self(self.head) or Head(None)
            commit = self(head.commit) or Commit()
            tree = self(commit.tree) or Tree({})
            tree.dags[dag] = self(Dag(set()))
            self.index = self(index, Index(self(Commit(commit.parent, self(tree)))))
            self.dag = dag

    def put_datum(self, value):
        with self.tx(True):
            if isinstance(value, (type(None), str, bool, int, float, Resource)):
                return self(Datum(value))
            elif isinstance(value, list):
                return self(Datum([self(Datum(x)) for x in value]))
            elif isinstance(value, set):
                return self(Datum({self(Datum(x)) for x in value}))
            elif isinstance(value, dict):
                return self(Datum({k: self(Datum(v)) for k, v in value.items()}))
            raise TypeError(f'unknown type: {type(value)}')

    def put_node(self, type, datum):
        with self.tx(True):
            commit = self(self(self.index).commit)
            tree = self(commit.tree)
            dag = self(tree.dags[self.dag])
            node = self(Node(type, datum))
            dag.nodes.add(node)
            tree.dags[self.dag] = self(dag)
            self(self.index, Index(self(Commit(commit.parent, self(tree)))))
            return node

    def commit(self, node):
        with self.tx(True):
            index = self(self.index)
            head = self(self.head)
            dag = self(Dag(self(self(self(index.commit).tree).dags[self.dag]).nodes, node))
            dags = self(self(head.commit).tree).dags if head else {}
            dags[self.dag] = dag
            self(self.head, Head(self(Commit(head.commit if head else None, self(Tree(dags))))))
            self.delete(self.index)
            self.dag = self.index = None
