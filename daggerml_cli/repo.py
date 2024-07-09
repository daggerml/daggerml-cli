import json
import logging
import os
import traceback as tb
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field, fields, is_dataclass, replace
from hashlib import md5
from typing import List
from uuid import uuid4

from daggerml_cli.db import db_type, dbenv
from daggerml_cli.pack import packb, register, unpackb
from daggerml_cli.util import asserting, makedirs, now

DEFAULT = 'head/main'
DATA_TYPE = {}


logger = logging.getLogger(__name__)
register(set, lambda x, _: sorted(list(x), key=packb), lambda x: [tuple(x)])


def from_json(text):
    return from_data(json.loads(text))


def to_json(obj):
    return json.dumps(to_data(obj), separators=(',', ':'))


def from_data(data):
    n, *args = data if isinstance(data, list) else [None, data]
    if n is None:
        return args[0]
    if n == 'l':
        return [from_data(x) for x in args]
    if n == 's':
        return {from_data(x) for x in args}
    if n == 'd':
        return {k: from_data(v) for (k, v) in args}
    if n in DATA_TYPE:
        return DATA_TYPE[n](*[from_data(x) for x in args])
    raise ValueError(f'no data encoding for type: {n}')


def to_data(obj):
    n = obj.__class__.__name__
    if isinstance(obj, (type(None), str, bool, int, float)):
        return obj
    if isinstance(obj, (list, set)):
        return [n[0], *[to_data(x) for x in obj]]
    if isinstance(obj, dict):
        return [n[0], *[[k, to_data(v)] for k, v in obj.items()]]
    if n in DATA_TYPE:
        return [n, *[to_data(getattr(obj, x.name)) for x in fields(obj)]]
    raise ValueError(f'no data encoding for type: {n}')


def unroll_datum(value):
    def get(value):
        if isinstance(value, Ref):
            return get(value())
        if isinstance(value, Datum):
            return get(value.value)
        if isinstance(value, (type(None), str, bool, int, float, Resource)):
            return value
        if isinstance(value, list):
            return [get(x) for x in value]
        if isinstance(value, set):
            return {get(x) for x in value}
        if isinstance(value, dict):
            return {k: get(v) for k, v in value.items()}
        raise TypeError(f'unroll_datum unknown type: {type(value)}')
    return get(value)


def repo_type(cls=None, **kwargs):
    """
    Teach MessagePack and LMDB how to serialize and deserialize classes

    Some of these classes are content-addressed, some are not.
    The content-addressed classes sometimes have extraneous fields that do not contribute to the id(entifier)
    Under the hood, this allows

    Parameters
    ----------
    cls: decorated class
    hash: hashed fields
    nohash: unhashed fields
    db: whether or not to create a top-level database

    Returns
    -------
    Decorated class

    """
    tohash = kwargs.pop('hash', None)
    nohash = kwargs.pop('nohash', [])
    dbtype = kwargs.pop('db', True)

    def packfn(x, hash):
        f = [y.name for y in fields(x)]
        if hash:
            f = [y for y in f if y not in nohash]
            f = [y for y in f if y in tohash] if tohash is not None else f
            if not len(f):
                return uuid4().hex
        return [getattr(x, y) for y in f]

    def decorator(cls):
        DATA_TYPE[cls.__name__] = cls
        register(cls, packfn, lambda x: x)
        return db_type(cls) if dbtype else cls

    return decorator(cls) if cls else decorator


@repo_type(db=False)
@dataclass(frozen=True, order=True)
class Ref:
    to: str | None = None

    @property
    def type(self):
        return self.to.split('/', 1)[0] if self.to else None

    @property
    def name(self):
        return self.to.split('/', 1)[1] if self.to else None

    def __call__(self):
        return Repo.curr.get(self)


@repo_type
@dataclass
class FnWaiter:
    expr: List[Ref]  # -> [Node]
    fndag: Ref  # -> FnDag
    dump: str|None = None

    def is_finished(self):
        return self.fndag().is_finished()


@repo_type(db=False)
@dataclass
class Error(Exception):
    message: str
    context: dict = field(default_factory=dict)
    code: str | None = None

    def __post_init__(self):
        self.code = type(self).__name__ if self.code is None else self.code

    @classmethod
    def from_ex(cls, ex):
        if isinstance(ex, Error):
            return ex
        formatted_tb = tb.format_exception(type(ex), value=ex, tb=ex.__traceback__)
        return cls(str(ex), {'trace': formatted_tb}, type(ex).__name__)


@repo_type(db=False)
@dataclass(frozen=True, slots=True)
class Resource:
    data: str


@repo_type(hash=[])
@dataclass
class Head:
    commit: Ref  # -> commit


@repo_type(hash=[])
@dataclass
class Index(Head):
    dag: Ref


@repo_type
@dataclass
class Commit:
    parents: list[Ref]  # -> commit
    tree: Ref  # -> tree
    cache: Ref  # -> tree
    author: str
    committer: str
    message: str
    created: str = field(default_factory=now)
    modified: str = field(default_factory=now)


@repo_type
@dataclass
class Tree:
    dags: dict[str, Ref]  # -> dag


@repo_type(hash=[])
@dataclass
class Dag:
    nodes: set[Ref]  # -> node
    result: Ref | None  # -> node
    error: Error | None

    def is_finished(self):
        return (self.result or self.error) is not None


@repo_type(hash=[])
@dataclass
class FnDag(Dag):
    expr: list[Ref]  # -> node
    meta: str = ''


@repo_type(hash=[])
@dataclass
class CachedFnDag(FnDag):

    @classmethod
    def from_fndag(cls, fndag):
        return cls(*[getattr(fndag, x.name) for x in fields(fndag)])


@repo_type(db=False)
@dataclass
class Literal:
    value: Ref  # -> datum

    @property
    def error(self):
        pass


@repo_type(db=False)
@dataclass
class Load:
    dag: Ref  # -> dag | fndag | cached_fndag

    @property
    def value(self):
        return self.dag().result().value

    @property
    def error(self):
        dag = self.dag()
        return dag.error


@repo_type(db=False)
@dataclass
class Fn(Load):
    expr: list[Ref]  # -> node


@repo_type
@dataclass
class Node:
    data: Literal | Load | Fn

    @property
    def value(self):
        return self.data.value

    @property
    def error(self):
        return self.data.error


@repo_type
@dataclass
class Datum:
    value: None | str | bool | int | float | Resource | list | dict | set


@dataclass
class Ctx:
    head: Head | Index
    commit: Commit
    tree: Tree
    cache: Tree
    dags: dict
    dag: Dag | None

    @classmethod
    def from_head(cls, ref, dag=None):
        head = asserting(ref())
        commit = head.commit()
        tree = commit.tree()
        cache = commit.cache()
        dags = tree.dags
        if dag is None and isinstance(head, Index):
            dag = head.dag
        if isinstance(dag, Ref):
            dag = dag()
        return cls(head, commit, tree, cache, dags, dag)


@repo_type(db=False)
@dataclass
class Repo:
    path: str
    user: str = 'unknown'
    head: Ref = field(default_factory=lambda: Ref(DEFAULT))  # -> head
    create: InitVar[bool] = False

    def __post_init__(self, create=False):
        self._tx = []
        dbfile = str(os.path.join(self.path, 'data.mdb'))
        dbfile_exists = os.path.exists(dbfile)
        if create:
            assert not dbfile_exists, f'repo exists: {dbfile}'
        else:
            assert dbfile_exists, f'repo not found: {dbfile}'
        self.env, self.dbs = dbenv(self.path)
        with self.tx(create):
            if not self.get('/init'):
                commit = Commit(
                    [],
                    self(Tree({})),
                    self(Tree({})),
                    self.user,
                    self.user,
                    'initial commit',
                )
                self(self.head, Head(self(commit)))
                self('/init', '00000000000000000000000000000000')  # so we all have a common root
            self.checkout(self.head)

    def __call__(self, key, obj=None):
        return self.put(key, obj)

    def db(self, type):
        return self.dbs[type] if type else None

    @contextmanager
    def tx(self, write=False):
        old_curr = getattr(Repo, 'curr', None)
        try:
            if not len(self._tx):
                self._tx.append(self.env.begin(write=write, buffers=True).__enter__())
                Repo.curr = self
            else:
                self._tx.append(None)
            yield True
        finally:
            Repo.curr = old_curr
            tx = self._tx.pop()
            if tx:
                tx.__exit__(None, None, None)

    def copy(self, path):
        self.env.copy(makedirs(path))

    def hash(self, obj):
        return md5(packb(obj, True)).hexdigest()

    def get(self, key):
        if key:
            key = key if isinstance(key, Ref) else Ref(key)
            if key.to:
                obj = unpackb(self._tx[0].get(key.to.encode(), db=self.db(key.type)))
                return obj

    def put(self, key, obj=None):
        key, obj = (key, obj) if obj else (obj, key)
        assert obj is not None
        key = key if isinstance(key, Ref) else Ref(key)
        db = key.type if key.to else type(obj).__name__.lower()
        data = packb(obj)
        key2 = key.to or f'{db}/{self.hash(obj)}'
        comp = None
        if key.to is None:
            comp = self._tx[0].get(key2.encode(), db=self.db(db))
            assert comp in [None, data], f'attempt to update immutable object: {key2}'
        if key is None or comp is None:
            self._tx[0].put(key2.encode(), data, db=self.db(db))
        return Ref(key2)

    def delete(self, key):
        key = Ref(key) if isinstance(key, str) else key
        self._tx[0].delete(key.to.encode(), db=self.db(key.type))

    def cursor(self, db):
        return map(lambda x: Ref(bytes(x[0]).decode()), iter(self._tx[0].cursor(db=self.db(db))))

    def walk(self, *key):
        result = set()
        xs = list(key)
        while len(xs):
            x = xs.pop(0)
            if isinstance(x, Ref):
                if x not in result:
                    result.add(x)
                    xs.append(x())
            elif isinstance(x, (list, set)):
                xs += [a for a in x if a not in result]
            elif isinstance(x, dict):
                xs += [a for a in x.values() if a not in result]
            elif is_dataclass(x):
                xs += [getattr(x, y.name) for y in fields(x)]
        return result

    def walk_ordered(self, *key):
        result = list()
        xs = list(key)
        while len(xs):
            x = xs.pop(0)
            if isinstance(x, Ref):
                if x not in result:
                    result.append(x)
                    xs.append(x())
            elif isinstance(x, (list, set)):
                xs += [a for a in x if a not in result]
            elif isinstance(x, dict):
                xs += [a for a in x.values() if a not in result]
            elif is_dataclass(x):
                xs += [getattr(x, y.name) for y in fields(x)]
        return list(reversed(result))

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
            assert len(pivot.parents), 'no merge base found'
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
        def merge_trees(base, a, b):
            return self.patch(a, self.diff(base, a), self.diff(base, b))
        c0 = self.merge_base(c1, c2)
        if c1 == c2:
            return c2
        if c0 == c2:
            return c1
        if c0 == c1:
            return c2
        return self(Commit(
            [c1, c2],
            merge_trees(c0().tree, c1().tree, c2().tree),
            merge_trees(c0().cache, c1().cache, c2().cache),
            author or self.user,
            self.user,
            message or f'merge {c2.name} with {c1.name}',
            created or now()))

    def rebase(self, c1, c2):
        def replay(commit):
            if commit == c0:
                return c1
            c = commit()
            p = c.parents
            assert len(p), f'commit has no parents: {commit.to}'
            if len(p) == 1:
                p, = p
                x = replay(p)
                c.tree = self.patch(x().tree, self.diff(p().tree, c.tree))
                c.cache = self.patch(x().cache, self.diff(p().cache, c.cache))
                c.parents, c.committer, c.modified = ([x], self.user, now())
                return self(c)
            assert len(p) == 2, f'commit has more than two parents: {commit.to}'
            a, b = (replay(x) for x in p)
            return self.merge(a, b, commit.author, commit.message, commit.created)
        c0 = self.merge_base(c1, c2)
        return c2 if c0 == c1 else c1 if c0 == c2 else replay(c2)

    def create_branch(self, branch, ref):
        assert branch.type == 'head', f'unexpected branch type: {branch.type}'
        assert branch() is None, 'branch already exists'
        assert ref.type in ['head', 'commit'], f'unexpected ref type: {ref.type}'
        ref = self(Head(ref)) if ref.type == 'commit' else ref
        return self(branch, ref())

    def delete_branch(self, branch):
        assert self.head != branch, 'cannot delete the current branch'
        assert self.get(branch) is not None, f'branch not found: {branch.to}'
        self.delete(branch)

    def set_head(self, head, commit):
        return self(head, Head(commit))

    def checkout(self, ref):
        assert ref.type in ['head'], f'checkout unknown ref type: {ref.type}'
        assert ref(), f'ref not found: {ref.to}'
        self.head = ref

    def put_datum(self, value):
        def put(value):
            if isinstance(value, Ref):
                if isinstance(value(), Node):
                    value = value().value
                assert isinstance(value(), Datum), f'not a datum: {value.to}'
                return value
            if isinstance(value, Datum):
                return self(value)
            if isinstance(value, (type(None), str, bool, int, float, Resource)):
                return self(Datum(value))
            if isinstance(value, list):
                return self(Datum([put(x) for x in value]))
            if isinstance(value, set):
                return self(Datum({put(x) for x in value}))
            if isinstance(value, dict):
                return self(Datum({k: put(v) for k, v in value.items()}))
            raise TypeError(f'repo put_datum unknown type: {type(value)}')
        return put(value)

    def get_dag(self, dag):
        return Ctx.from_head(self.head).dags.get(dag)

    def begin(self, *, name, message, dag=None):
        ctx = Ctx.from_head(self.head)
        if dag is None:
            dag = self(Dag(set(), None, None))
        ctx.dags[name] = dag
        commit = Commit(
            [ctx.head.commit],
            self(ctx.tree),
            self(ctx.cache),
            self.user,
            self.user,
            message)
        index = self(Index(self(commit), dag))
        return index

    def put_node(self, data, index: Ref):
        ctx = Ctx.from_head(index)
        node = self(Node(data))
        ctx.dag.nodes.add(node)
        self(ctx.head.dag, ctx.dag)
        ctx.commit.tree = self(ctx.tree)
        ctx.commit.created = ctx.commit.modified = now()
        self(index, Index(self(ctx.commit), ctx.head.dag))
        return node

    def get_node_value(self, ref: Ref):
        node = ref()
        assert isinstance(node, Node), f'invalid type: {type(node)}'
        if node.error is not None:
            return node.error
        val = node.value()
        assert isinstance(val, Datum)
        return unroll_datum(val)

    def dump_ref(self, ref):
        objs = [[x, x()] for x in self.walk_ordered(ref)]
        return to_json(objs)

    def load_ref(self, ref_dump):
        *_, new_dag = (self.put(a, b) for a, b in from_json(ref_dump))
        return new_dag

    def start_fn(self, *, expr, index, use_cache=False):
        datum_expr = [x().value for x in expr]
        dag = index().dag
        ctx = Ctx.from_head(index, dag=dag)
        fndag = None
        if use_cache:
            cache_key = self.hash(datum_expr)
            fndag = ctx.cache.dags.get(cache_key)
            # if cache and fndag and fndag().is_finished() and not retry:
            if fndag is not None:
                assert fndag().is_finished(), f"cached {fndag = } was not finished!"
                logger.debug('using finished cached dag: %r', fndag())
                fndag = self(CachedFnDag.from_fndag(fndag()))
        if fndag is None:
            logger.debug('starting new fndag')
            fndag = self(FnDag(set(), None, None, expr))
            ctx.commit.parents = [ctx.head.commit]
            ctx.commit.modified = now()
            self(index, Index(self(ctx.commit), ctx.head.dag))
        out = self.dump_ref(fndag)
        waiter = self(FnWaiter(expr, fndag, dump=out))
        return waiter

    def populate_cache(self, index, waiter):
        waiter = waiter()
        if isinstance(waiter, FnWaiter):
            dag = waiter.fndag
        elif isinstance(waiter, Node):
            dag = waiter.data.dag
        else:
            msg = f'invalid type passed to populate_cache ({type(waiter)})'
            raise ValueError(msg)
        cache_key = self.hash([x().value for x in dag().expr])
        ctx = Ctx.from_head(index)
        ctx.cache.dags[cache_key] = self(CachedFnDag.from_fndag(dag()))
        ctx.commit.cache = self(ctx.cache)
        ctx.commit.modified = now()
        commit = self(ctx.commit)
        if isinstance(ctx.head, Index):
            head = Index(commit, ctx.head.dag)
            self(index, head)
            return
        self.set_head(self.head, commit)

    def get_fn_result(self, index, waiter):
        waiter = waiter()
        assert isinstance(waiter, FnWaiter)
        if not waiter.is_finished():
            return
        fn = Fn(dag=waiter.fndag, expr=waiter.expr)
        return self.put_node(fn, index=index)

    def get_fn_meta(self, fndag_ref: Ref) -> str:
        fndag = fndag_ref()
        assert isinstance(fndag, FnDag)
        return fndag.meta

    def update_fn_meta(self, fndag_ref: Ref, old_meta: str, new_meta: str) -> None:
        fndag = fndag_ref()
        assert isinstance(fndag, FnDag)
        if fndag.meta != old_meta:
            raise Error('old metadata', code='old-metadata')
        self(fndag_ref, replace(fndag, meta=new_meta))

    def commit(self, res_or_err, index: Ref):
        result, error = (res_or_err, None) if isinstance(res_or_err, Ref) else (None, res_or_err)
        assert result is not None or error is not None, 'both result and error are none'
        dag = index().dag
        ctx = Ctx.from_head(index, dag=dag)
        assert (ctx.dag.result or ctx.dag.error) is None, 'dag has been committed already'
        ctx.dag.result = result
        ctx.dag.error = error
        ctx.commit.tree = self(ctx.tree)
        ctx.commit.created = ctx.commit.modified = now()
        dag_ref = self(dag, ctx.dag)
        commit = self.merge(self.head().commit, self(ctx.commit))
        self.set_head(self.head, commit)
        self.delete(index)
        return dag_ref
