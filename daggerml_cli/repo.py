import asciidag.graph
import asciidag.node
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


@repo_type
class Tree:
    dags: dict


@repo_type
class Dag:
    nodes: set
    result: Ref
    error: dict | None


@repo_type
class Node:
    type: str
    expr: list
    value: Ref


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
                self(self.head, Head(self(Commit([], self(Tree({}))))))
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

    def log(self, db=None, ref=None):
        if db:
            result = []
            for (k, _) in self.cursor(db):
                result.append(self.log(ref=Ref(bytes(k).decode())().commit))
            return result
        if ref:
            return [ref.to, [self.log(ref=x) for x in ref().parents]]

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
            if y and x not in result:
                result.append(x)
                ys += y.parents
        return self.ancestors(ys, result) if len(ys) else result

    def common_ancestor(self, a, b):
        aa = self.ancestors(a)
        ab = self.ancestors(b)
        if set(aa).issubset(ab):
            return a
        pivot = max(set(aa).difference(ab), key=aa.index)()
        if len(pivot.parents) > 1:
            return self.common_ancestor(*pivot.parents)
        return list(pivot.parents)[0]

    def diff(self, t1, t2):
        d1 = t1().dags
        d2 = t2().dags
        result = {'add': {}, 'rem': {}}
        for k in set(d1.keys()).union(d2.keys()):
            if k not in d2:
                result['rem'][k] = d1[k]
            elif k not in d1:
                result['add'][k] = d2[k]
            elif d1[k] != d2[k]:
                result['rem'][k] = d1[k]
                result['add'][k] = d2[k]
        return result

    def patch(self, tree, *diffs):
        diff = {'add': {}, 'rem': {}}
        tree = tree()
        for d in diffs:
            diff['add'].update(d['add'])
            diff['rem'].update(d['rem'])
        [tree.dags.pop(k, None) for k in diff['rem'].keys()]
        tree.dags.update(diff['add'])
        return self(tree)

    def merge(self, c1, c2):
        c0 = self.common_ancestor(c1, c2)
        if c1 == c2:
            return c2
        if c0 == c2:
            return c1
        if c0 == c1:
            return c2
        d1 = self.diff(c0().tree, c1().tree)
        d2 = self.diff(c0().tree, c2().tree)
        tree = self.patch(c1().tree, d1, d2)
        return self(Commit([c1, c2], tree))

    def rebase(self, c1, c2):
        c0 = self.common_ancestor(c1, c2)
        if c0 == c1:
            return c2
        if c0 == c2:
            return c1

        def replay(commit):
            if commit == c0:
                return c1
            p = commit().parents
            if len(p) == 1:
                p, = p
                x = replay(p)
                diff = self.diff(p().tree, commit().tree)
                tree = self.patch(x().tree, diff)
                return self(Commit([x], tree))
            assert len(p) == 2
            a, b = (replay(x) for x in p)
            return self.merge(a, b)

        return replay(c2)

    def squash(self, commit):
        pass

    def create_branch(self, branch, ref):
        assert branch.type == 'head'
        assert branch() is None
        assert ref.type in ['head', 'commit']
        ref = self(Head(ref)) if ref.type == 'commit' else ref
        return self(branch, ref())

    def set_head(self, head, commit):
        return self(head, Head(commit))

    def checkout(self, ref):
        assert ref.type in ['head'], f'unknown ref type: {ref.type}'
        assert ref(), f'no such ref: {ref.to}'
        self.head = ref

    def begin(self, dag):
        head = self.head()
        commit = head.commit()
        tree = commit.tree()
        tree.dags[dag] = self(Dag(set(), Ref(None), None))
        self.index = self(Index(self(Commit([head.commit], self(tree)))))
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

    def put_node(self, type, expr, datum):
        commit = self.index().commit()
        tree = commit.tree()
        dag = tree.dags[self.dag]()
        node = self(Node(type, expr, datum))
        dag.nodes.add(node)
        tree.dags[self.dag] = self(dag)
        self.index = self(self.index, Index(self(Commit(commit.parents, self(tree)))))
        return node

    def commit(self, res_or_err):
        result, error = (res_or_err, None) if isinstance(res_or_err, Ref) else (None, res_or_err)
        index = self.index()
        commit = index.commit()
        tree = commit.tree()
        dag = tree.dags[self.dag]()
        dag.result = result
        dag.error = error
        tree.dags[self.dag] = self(dag)
        commit = self.rebase(self.head().commit, self(Commit(commit.parents, self(tree))))
        self.set_head(self.head, commit)
        self.delete(self.index)
        self.index = Ref(None)
        self.dag = None

    def graph(self):
        def walk(x):
            if x and x[0]:
                return asciidag.node.Node(x[0], parents=[walk(y) for y in x[1]])
        heads = [walk(x) for x in self.log('head')]
        asciidag.graph.Graph().show_nodes(heads)
