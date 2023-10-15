import os
from daggerml_cli.db import dbenv, db_type
from daggerml_cli.pack import packb, unpackb, packb64, unpackb64, register
from daggerml_cli.util import now
from dataclasses import dataclass, fields, is_dataclass
from hashlib import md5
from uuid import uuid4


DEFAULT = 'head/main'


register(set, lambda x, h: sorted(list(x), key=packb), lambda x: [tuple(x)])


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


@repo_type(db=False, frozen=True)
class Ref:
    to: str

    @property
    def type(self):
        return self.to.split('/', 1)[0] if self.to else None

    @property
    def name(self):
        return self.to.split('/', 1)[1] if self.to else None

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
    parents: list[Ref]
    tree: Ref
    author: str
    committer: str
    message: str
    dag: str = None
    created: str = None
    modified: str = None

    def __post_init__(self):
        if self.created is None:
            self.created = now()
        if self.modified is None:
            self.modified = now()


@repo_type
class Tree:
    dags: dict[str, Ref]


@repo_type
class Dag:
    nodes: set[Ref]
    result: Ref
    error: dict | None


@repo_type
class Node:
    type: str
    expr: list[Ref]
    value: Ref


@repo_type
class Datum:
    value: type(None) | str | bool | int | float | Resource | list | dict | set


@dataclass
class Ctx:
    ref: Ref
    head: Head
    commit: Commit
    tree: Tree
    dags: dict
    dag: Dag


@dataclass
class Repo:
    path: str
    user: str = None
    head: Ref = Ref(DEFAULT)
    index: Ref = Ref(None)
    dag: str = None
    create: bool = False

    @classmethod
    def new(cls, b64state):
        return cls(*unpackb64(b64state))

    @property
    def state(self):
        return packb64([getattr(self, x.name) for x in fields(self)])

    def __post_init__(self):
        dbfile = str(os.path.join(self.path, 'data.mdb'))
        dbfile_exists = os.path.exists(dbfile)
        if self.create:
            assert not dbfile_exists, f'repo exists: {dbfile}'
        else:
            assert dbfile_exists, f'repo not found: {dbfile}'
        self.env, self.dbs = dbenv(self.path)
        with self.tx(self.create):
            if not self.get('/init'):
                commit = Commit(
                    [],
                    self(Tree({})),
                    self.user,
                    self.user,
                    'initial commit',
                )
                self(self.head, Head(self(commit)))
                self('/init', uuid4().hex)
            self.checkout(self.head)

    def __call__(self, key, obj=None):
        return self.put(key, obj)

    def db(self, type):
        return self.dbs[type] if type else None

    def tx(self, write=False):
        tx = self._tx = self.env.begin(write=write, buffers=True)
        Repo.curr = self
        return tx

    def copy(self, path):
        os.makedirs(path, mode=0o700, exist_ok=True)
        self.env.copy(path)

    def ctx(self, ref, dag=None):
        head = ref()
        commit = head.commit() if head else None
        tree = commit.tree() if commit else None
        dags = tree.dags if tree else None
        dag = dags[dag]() if dags and dag in dags else None
        return Ctx(ref, head, commit, tree, dags, dag)

    def hash(self, obj):
        return md5(packb(obj, True)).hexdigest()

    def get(self, key):
        if key:
            key = Ref(key) if isinstance(key, str) else key
            if key.to:
                obj = unpackb(self._tx.get(key.to.encode(), db=self.db(key.type)))
                return obj

    def put(self, key, obj=None):
        key, obj = (key, obj) if obj else (obj, key)
        key = Ref(None) if key is None else (Ref(key) if isinstance(key, str) else key)
        if obj is not None:
            db = key.type if key.to else obj.__class__.__name__.lower()
            data = packb(obj)
            key2 = key.to or f'{db}/{self.hash(obj)}'
            comp = None
            if key.to is None:
                comp = self._tx.get(key2.encode(), db=self.db(db))
                assert comp is None or comp == data, f'attempt to update immutable object: {key2}'
            if key is None or comp is None:
                self._tx.put(key2.encode(), data, db=self.db(db))
            return Ref(key2)
        return Ref(None)

    def delete(self, key):
        self._tx.delete(key.to.encode(), db=self.db(key.type))

    def cursor(self, db):
        return map(lambda x: Ref(bytes(x[0]).decode()), iter(self._tx.cursor(db=self.db(db))))

    def walk(self, *key):
        result = set()
        xs = list(key)
        while len(xs):
            x = xs.pop(0)
            if isinstance(x, Ref):
                result.add(x)
                xs.append(x())
            elif isinstance(x, (list, set)):
                xs += x
            elif isinstance(x, dict):
                xs += x.values()
            elif is_dataclass(x):
                xs += [getattr(x, y.name) for y in fields(x)]
        return result

    def heads(self):
        return [k for k in self.cursor('head')]

    def indexes(self):
        return [k for k in self.cursor('index')]

    def log(self, db=None, ref=None):
        def sort(xs):
            return reversed(sorted(xs, key=lambda x: x().modified))
        if db:
            return {k: self.log(ref=k().commit) for k in self.cursor(db)}
        if ref and ref.to:
            return [ref, [self.log(ref=x) for x in sort(ref().parents) if x and x.to]]

    def objects(self):
        result = set()
        for db in self.dbs.keys():
            [result.add(k) for k in self.cursor(db)]
        return result

    def reachable_objects(self):
        result = set()
        for db in ['head', 'index']:
            result = result.union(self.walk(*[k for k in self.cursor(db)]))
        return result

    def unreachable_objects(self):
        return self.objects().difference(self.reachable_objects())

    def gc(self):
        to_delete = self.unreachable_objects()
        [self.delete(k) for k in to_delete]
        return len(to_delete)

    def topo_sort(self, *xs):
        xs = list(xs)
        result = []
        while len(xs):
            x = xs.pop(0)
            if x is not None and x() and x not in result:
                result.append(x)
                xs = x().parents + xs
        return result

    def merge_base(self, a, b):
        while True:
            aa = self.topo_sort(a)
            ab = self.topo_sort(b)
            if set(aa).issubset(ab):
                return a
            if set(ab).issubset(aa):
                return b
            pivot = max(set(aa).difference(ab), key=aa.index)()
            if len(pivot.parents) == 1:
                return pivot.parents[0]
            a, b = pivot.parents

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

    def merge(self, c1, c2, author=None, message=None, created=None):
        c0 = self.merge_base(c1, c2)
        if c1 == c2:
            return c2
        if c0 == c2:
            return c1
        if c0 == c1:
            return c2
        d1 = self.diff(c0().tree, c1().tree)
        d2 = self.diff(c0().tree, c2().tree)
        return self(Commit(
            [c1, c2],
            self.patch(c1().tree, d1, d2),
            author or self.user,
            self.user,
            message or f'merge {c2.name} with {c1.name}',
            None,
            created or now(),
        ))

    def rebase(self, c1, c2):
        def replay(commit):
            if commit == c0:
                return c1
            c = commit()
            p = c.parents
            assert len(p), f'commit has no parents: {commit.to}'
            if len(p) == 1:
                x = replay(p[0])
                c.tree = self.patch(x().tree, self.diff(p[0]().tree, commit().tree))
                c.parents, c.committer, c.modified = ([x], self.user, now())
                return self(c)
            assert len(p) == 2, f'commit has more than two parents: {commit.to}'
            a, b = (replay(x) for x in p)
            return self.merge(a, b, commit.author, commit.message, commit.created)
        c0 = self.merge_base(c1, c2)
        return c2 if c0 == c1 else c1 if c0 == c2 else replay(c2)

    def squash(self, commit):
        pass

    def create_branch(self, branch, ref):
        assert branch.type == 'head', f'unexpected branch type: {branch.type}'
        assert branch() is None, 'branch already exists'
        assert ref.type in ['head', 'commit'], f'unexpected ref type: {ref.type}'
        ref = self(Head(ref)) if ref.type == 'commit' else ref
        return self(branch, ref())

    def delete_branch(self, branch):
        assert self.head != branch, 'cannot delete the current branch'
        self.delete(branch)

    def set_head(self, head, commit):
        return self(head, Head(commit))

    def checkout(self, ref):
        assert ref.type in ['head'], f'unknown ref type: {ref.type}'
        assert ref(), f'no such ref: {ref.to}'
        self.head = ref

    def begin(self, dag, message):
        ctx = self.ctx(self.head, dag)
        ctx.dags[dag] = self(Dag(set(), Ref(None), None))
        self.index = self(Index(self(Commit(
            [ctx.head.commit],
            self(ctx.tree),
            self.user,
            self.user,
            message,
            dag))))
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
        ctx = self.ctx(self.index, self.dag)
        node = self(Node(type, expr, datum))
        ctx.dag.nodes.add(node)
        ctx.dags[self.dag] = self(ctx.dag)
        ctx.commit.tree = self(ctx.tree)
        ctx.commit.created = ctx.commit.modified = now()
        self.index = self(self.index, Index(self(ctx.commit)))
        return node

    def commit(self, res_or_err):
        result, error = (res_or_err, None) if isinstance(res_or_err, Ref) else (None, res_or_err)
        ctx = self.ctx(self.index, self.dag)
        ctx.dag.result = result
        ctx.dag.error = error
        ctx.tree.dags[self.dag] = self(ctx.dag)
        ctx.commit.tree = self(ctx.tree)
        ctx.commit.created = ctx.commit.modified = now()
        commit = self.merge(self.head().commit, self(ctx.commit))
        self.set_head(self.head, commit)
        self.delete(self.index)
        self.index = Ref(None)
        self.dag = None
