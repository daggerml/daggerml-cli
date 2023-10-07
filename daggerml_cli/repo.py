from daggerml_cli.db import dbenv, db_type
from daggerml_cli.pack import packb, unpackb, packb64, unpackb64, packb_type
from daggerml_cli.util import now, walk_type, uuid_type, walk_fields, is_uuid
from dataclasses import dataclass, field, fields
from hashlib import md5
from uuid import uuid4


DEFAULT = 'head/main'


@packb_type
class Resource:
    data: dict


@db_type
@packb_type
@walk_type('commit')
class Index:
    commit: str


@db_type
@uuid_type
@packb_type
@walk_type('commit')
class Head:
    commit: str


@db_type
@packb_type
@walk_type('parent', 'tree', 'meta')
class Commit:
    parent: str
    tree: str
    timestamp: str
    meta: str = None


@db_type
@packb_type
@walk_type('dags')
class Tree:
    dags: dict


@db_type
@packb_type
@walk_type('nodes', 'result')
class Dag:
    nodes: set
    result: str
    error: dict


@db_type
@uuid_type
@packb_type
@walk_type('value', 'meta')
class Node:
    type: str
    value: str
    meta: str = None


@db_type
@packb_type
@walk_type(lambda x: ['value'] if isinstance(x, (list, dict, set)) else [])
class Datum:
    value: type(None) | str | bool | int | float | Resource | list | dict | set


@db_type
@packb_type
@walk_type('value')
class Meta:
    value: str


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
            obj = unpackb(self._tx.get(key.encode(), db=self.db[db]))
            if obj and hasattr(obj, 'meta'):
                obj.meta = self(f'meta/{key}')
            return obj
        key, obj = (key, obj) if obj else (obj, key)
        if obj is not None:
            db = obj.__class__.__name__.lower()
            data = packb(obj)
            key2 = key or f'{db}/{uuid4().hex if is_uuid(obj) else md5(data).hexdigest()}'
            comp = None
            if key is None and not is_uuid(obj):
                comp = self._tx.get(key2.encode(), db=self.db[db])
                assert comp is None or comp == data
            if key is None or comp is None:
                self._tx.put(key2.encode(), data, db=self.db[db])
                self(f'meta/{key2}', obj.meta) if hasattr(obj, 'meta') and obj.meta else None
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
            d = self(key)
            for field_name in walk_fields(d):
                f = getattr(d, field_name)
                if isinstance(f, str):
                    self.walk(f, result)
                elif isinstance(f, (list, set)):
                    [self.walk(k, result) for k in f]
                elif isinstance(f, dict):
                    [self.walk(k, result) for k in f.values()]
                if field_name == 'meta':
                    self.walk(f'meta/{key}', result)
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
        with self.tx(True):
            head = self(self.head) or Head(None)
            commit = self(head.commit) or Commit(None, None, now())
            tree = self(commit.tree) or Tree({})
            tree.dags[dag] = self(Dag(set(), None, None))
            self.index = self(Index(self(Commit(commit.parent, self(tree), now()))))
            self.dag = dag

    def put_datum(self, value):
        with self.tx(True):
            def put(value):
                if isinstance(value, (type(None), str, bool, int, float, Resource)):
                    return self(Datum(value))
                elif isinstance(value, list):
                    return self(Datum([put(x) for x in value]))
                elif isinstance(value, set):
                    return self(Datum({put(x) for x in value}))
                elif isinstance(value, dict):
                    return self(Datum({k: put(v) for k, v in value.items()}))
                raise TypeError(f'unknown type: {type(value)}')
            return put(value)

    def put_node(self, type, datum, meta=None):
        with self.tx(True):
            commit = self(self(self.index).commit)
            tree = self(commit.tree)
            dag = self(tree.dags[self.dag])
            node = self(Node(type, datum, meta=meta))
            dag.nodes.add(node)
            tree.dags[self.dag] = self(dag)
            self(self.index, Index(self(Commit(commit.parent, self(tree), now()))))
            return node

    def commit(self, node, meta=None):
        with self.tx(True):
            node, error = (node, None) if isinstance(node, str) else (None, node)
            index = self(self.index)
            head = self(self.head)
            dag = self(Dag(self(self(self(index.commit).tree).dags[self.dag]).nodes, node, error))
            dags = self(self(head.commit).tree).dags if head else {}
            dags[self.dag] = dag
            self(self.head, Head(self(Commit(head.commit if head else None, self(Tree(dags)), now(), meta=meta))))
            self.delete(self.index)
            self.dag = self.index = None
