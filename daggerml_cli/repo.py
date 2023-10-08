from daggerml_cli.db import dbenv, db_type
from daggerml_cli.pack import packb, unpackb, packb64, unpackb64, register
from daggerml_cli.util import now
from dataclasses import dataclass, fields, is_dataclass
from hashlib import md5
from uuid import uuid4


DEFAULT = 'head/main'


register(set, lambda x, h: sorted(list(x)), lambda x: [tuple(x)])


def repo_type(cls=None, **kwargs):
    tohash = kwargs.pop('hash', None)
    nohash = kwargs.pop('nohash', [])
    dbtype = kwargs.pop('db', True)

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
        return dataclass(**kwargs)(db_type(cls) if dbtype else cls)

    return decorator(cls) if cls else decorator


@repo_type(db=False)
class Resource:
    data: dict


@repo_type(frozen=True)
class Ref:
    to: str

    @property
    def type(self):
        return self.to.split('/')[0] if self.to else None

    def __call__(self):
        return Repo.curr.get(self)


@repo_type(hash=[])
class Index:
    commit: Ref


@repo_type(hash=[])
class Head:
    commit: Ref


@repo_type
class Commit:
    parents: set[Ref]
    tree: Ref
    timestamp: str


@repo_type
class Tree:
    dags: dict


@repo_type
class Dag:
    nodes: set
    result: Ref
    error: dict | None
    meta: dict | None = None


@repo_type(hash=[])
class Node:
    type: str
    value: Ref
    meta: Ref = Ref(None)


@repo_type
class Datum:
    value: type(None) | str | bool | int | float | Resource | list | dict | set


@dataclass
class Repo:
    path: str
    head: Ref = Ref(DEFAULT)
    index: Ref = Ref(None)
    dag: str = None

    @classmethod
    def new(cls, b64state):
        return cls(*unpackb64(b64state))

    @property
    def state(self):
        return packb64([getattr(self, x.name) for x in fields(self)])

    def __post_init__(self):
        self.env, self.dbs = dbenv(self.path)
        self._tx = None
        with self.tx(True):
            if not self.get(Ref('/init')):
                self(self.head, Head(self(Commit([Ref(None)], self(Tree({})), now()))))
                self(Ref('/init'), True)
            self.checkout(self.head)

    def __call__(self, key, obj=None):
        return self.put(key, obj)

    def db(self, type):
        return self.dbs[type] if type else None

    def tx(self, write=False):
        tx = self._tx = self.env.begin(write=write, buffers=True)
        Repo.curr = self
        return tx

    def hash(self, obj):
        return md5(packb(obj, True)).hexdigest()

    def get(self, key):
        if key and key.to:
            obj = unpackb(self._tx.get(key.to.encode(), db=self.db(key.type)))
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
                comp = self._tx.get(key2.encode(), db=self.db(db))
                assert comp is None or comp == data
            if key is None or comp is None:
                self._tx.put(key2.encode(), data, db=self.db(db))
            return Ref(key2)
        return Ref(None)

    def delete(self, key):
        self._tx.delete(key.to.encode(), db=self.db(key.type))

    def cursor(self, db):
        return iter(self._tx.cursor(db=self.db(db)))

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

    def objects(self):
        result = set()
        for db in self.dbs.keys():
            for (k, _) in self.cursor(db):
                result.add(bytes(k).decode())
        return result

    def reachable_objects(self):
        result = set()
        for db in ['head', 'index']:
            for (k, _) in self.cursor(db):
                self.walk(Ref(bytes(k).decode()), result)
        return result

    def unreachable_objects(self):
        return self.objects().difference(self.reachable_objects())

    def gc(self):
        [self.delete(Ref(k)) for k in self.unreachable_objects()]

    def ancestors(self, xs, result=None):
        xs = xs if isinstance(xs, list) else [xs]
        result = [] if result is None else result
        ys = []
        for x in [x for x in xs if x is not None]:
            y = x()
            if y:
                result.append(x)
                ys += y.parents
        return self.ancestors(ys, result) if len(ys) else result

    def common_ancestor(self, a, b):
        aa = self.ancestors(a)
        ab = self.ancestors(b)
        sa = set(tuple(aa))
        sb = set(tuple(ab))

        for x in aa:
            if sa.issubset(sb):
                return x
            sa.remove(x)

    def create_branch(self, branch, ref):
        assert branch.type == 'head'
        assert branch() is None
        assert ref.type in ['head', 'commit']
        ref = self(Head(ref)) if ref.type == 'commit' else ref
        return self(branch, ref())

    def checkout(self, ref):
        assert ref.type in ['head'], f'unknown ref type: {ref.type}'
        assert ref(), f'no such ref: {ref.to}'
        self.head = ref

    def merge(self, a, b):
        c = self.common_ancestor(a, b)
        print(c)

    def begin(self, dag, meta=None):
        head = self.head() or Head(Ref(None))
        commit = head.commit() or Commit([Ref(None)], Ref(None), now())
        tree = commit.tree() or Tree({})
        tree.dags[dag] = self(Dag(set(), Ref(None), None, meta=meta))
        self.index = self(Index(self(Commit(commit.parents, self(tree), now()))))
        self.dag = dag

    def put_datum(self, value):
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
        commit = self.index().commit()
        tree = commit.tree()
        dag = tree.dags[self.dag]()
        node = self(Node(type, datum, meta=meta))
        dag.nodes.add(node)
        tree.dags[self.dag] = self(dag)
        self(self.index, Index(self(Commit(commit.parents, self(tree), now()))))
        return node

    def commit(self, res_or_err):
        result, error = (res_or_err, None) if isinstance(res_or_err, Ref) else (None, res_or_err)
        index = self.index()
        head = self.head()
        dag = index.commit().tree().dags[self.dag]()
        dag.result = result
        dag.error = error
        dags = head.commit().tree().dags if head else {}
        dags[self.dag] = self(dag)
        self(self.head, Head(self(Commit([head.commit] if head else [Ref(None)], self(Tree(dags)), now()))))
        self.delete(self.index)
        self.index = Ref(None)
        self.dag = None
