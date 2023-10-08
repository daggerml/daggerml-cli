from daggerml_cli.db import dbenv, db_type
from daggerml_cli.pack import packb, unpackb, packb64, unpackb64, register
from daggerml_cli.util import now
from dataclasses import dataclass, fields, is_dataclass, _MISSING_TYPE
from hashlib import md5
from uuid import uuid4


DEFAULT = 'head/main'


register(set, lambda x, h: sorted(list(x)), lambda x: [tuple(x)])


def packb_type(cls=None, **kwargs):
    tohash = kwargs.pop('hash', None)
    nohash = kwargs.pop('nohash', [])

    def packfn(x, hash):
        f = [y.name for y in fields(x)]
        if hash:
            f = [y for y in f if y not in nohash]
            f = [y for y in f if y in tohash] if tohash else f
            if not len(f):
                return uuid4().hex
        return [getattr(x, y) for y in f]

    def decorator(cls):
        register(cls, packfn, lambda x: x)
        return dataclass(**kwargs)(cls)
    return decorator(cls) if cls else decorator


@packb_type
class Resource:
    data: dict


@packb_type(frozen=True)
class Ref:
    to: str

    @property
    def type(self):
        return self.to.split('/')[0] if self.to else None

    def __call__(self):
        return Repo.curr.get(self)


@db_type
@packb_type
class Index:
    commit: Ref


@db_type
@packb_type(hash=[])
class Head:
    commit: Ref


@db_type
@packb_type
class Commit:
    parent: Ref
    tree: Ref
    timestamp: str


@db_type
@packb_type
class Tree:
    dags: dict


@db_type
@packb_type(nohash=['meta'])
class Dag:
    nodes: set
    result: Ref
    error: dict | None
    meta: dict | None = None


@db_type
@packb_type(hash=[])
class Node:
    type: str
    value: Ref
    meta: Ref = Ref(None)


@db_type
@packb_type
class Datum:
    value: type(None) | str | bool | int | float | Resource | list | dict | set


@dataclass
class Repo:
    path: str
    head: Ref = Ref(DEFAULT)
    index: Ref = Ref(None)
    dag: str = None

    def __post_init__(self):
        self.env, self.db = dbenv(self.path)
        self._tx = None

    def __call__(self, key, obj=None):
        return self.put(key, obj)

    def tx(self, write=False):
        tx = self._tx = self.env.begin(write=write, buffers=True)
        Repo.curr = self
        return tx

    def hash(self, obj):
        return md5(packb(obj, True)).hexdigest()

    def get(self, key):
        if key and key.to:
            obj = unpackb(self._tx.get(key.to.encode(), db=self.db[key.type]))
            return obj

    def put(self, key, obj=None):
        key, obj = (key, obj) if obj else (obj, key)
        key = Ref(None) if key is None else key
        if obj is not None:
            db = key.type if key.to else obj.__class__.__name__.lower()
            data = packb(obj)
            key2 = key.to or f'{db}/{self.hash(obj)}'
            comp = None
            if key.to is None:
                comp = self._tx.get(key2.encode(), db=self.db[db])
                assert comp is None or comp == data
            if key is None or comp is None:
                self._tx.put(key2.encode(), data, db=self.db[db])
            return Ref(key2)
        return Ref(None)

    def delete(self, key):
        self._tx.delete(key.to.encode(), db=self.db[key.type])

    def cursor(self, db):
        return iter(self._tx.cursor(db=self.db[db]))

    def walk(self, key, result=None):
        result = set() if result is None else result
        if isinstance(key, Ref):
            result.add(key.to)
            self.walk(key(), result)
        elif isinstance(key, (list, set)):
            [self.walk(x, result) for x in key]
        elif isinstance(key, dict):
            self.walk(list(key.values()), result)
        elif is_dataclass(key):
            [self.walk(getattr(key, x.name), result) for x in fields(key)]
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
                    self.walk(Ref(bytes(k).decode()), live_objs)
            for db in self.db.keys():
                for (k, _) in self.cursor(db):
                    k = bytes(k).decode()
                    self.delete(Ref(k)) if k not in live_objs else None

    def begin(self, dag, meta=None):
        with self.tx(True):
            head = self.head() or Head(Ref(None))
            commit = head.commit() or Commit(Ref(None), Ref(None), now())
            tree = commit.tree() or Tree({})
            tree.dags[dag] = self(Dag(set(), Ref(None), None, meta=meta))
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
            commit = self.index().commit()
            tree = commit.tree()
            dag = tree.dags[self.dag]()
            node = self(Node(type, datum, meta=meta))
            dag.nodes.add(node)
            tree.dags[self.dag] = self(dag)
            self(self.index, Index(self(Commit(commit.parent, self(tree), now()))))
            return node

    def commit(self, res_or_err):
        with self.tx(True):
            result, error = (res_or_err, None) if isinstance(res_or_err, Ref) else (None, res_or_err)
            index = self.index()
            head = self.head()
            dag = index.commit().tree().dags[self.dag]()
            dag.result = result
            dag.error = error
            dags = head.commit().tree().dags if head else {}
            dags[self.dag] = self(dag)
            self(self.head, Head(self(Commit(head.commit if head else Ref(None), self(Tree(dags)), now()))))
            self.delete(self.index)
            self.index = Ref(None)
            self.dag = None
