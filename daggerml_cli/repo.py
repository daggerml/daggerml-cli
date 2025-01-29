import json
import logging
import os
import shutil
import subprocess
import traceback as tb
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field, fields, is_dataclass
from hashlib import md5
from typing import Dict, Optional, Union
from urllib.parse import urlparse
from uuid import uuid4

from daggerml_cli.db import db_type, dbenv
from daggerml_cli.pack import packb, register, unpackb
from daggerml_cli.util import asserting, assoc, conj, makedirs, now

DEFAULT_BRANCH = 'head/main'
DATA_TYPE = {}
NONE = {}


BUILTIN_FNS = {
    'type': lambda x: str(type(x).__name__),
    'len': lambda x: len(x),
    'keys': lambda x: sorted(x.keys()),
    'get': lambda x, k, d=NONE: x[slice(*[x().value for x in k])] if isinstance(k, list) else x[k] if d is NONE else x.get(k, d),
    'contains': lambda x, k: k in unroll_datum(x),
    'list': lambda *xs: list(xs),
    'dict': lambda *kvs: {k: v for k, v in [kvs[i:i + 2] for i in range(0, len(kvs), 2)]},
    'set': lambda *xs: set(xs),
    'assoc': assoc,
    'conj': conj,
    'build': lambda x, *_: x,
}

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
    if isinstance(obj, tuple):
        obj = list(obj)
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


def raise_ex(x):
    if isinstance(x, Exception):
        raise x
    return x


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
    to: Optional[str] = None

    @property
    def type(self):
        return self.to.split('/', 1)[0] if self.to else None

    @property
    def id(self):
        return self.to.split('/', 1)[1] if self.to else None

    def __call__(self):
        return Repo.curr.get(self)


@repo_type(db=False)
@dataclass
class Error(Exception):
    message: Union[str, Exception]
    context: dict = field(default_factory=dict)
    code: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.message, Error):
            ex = self.message
            self.message = ex.message
            self.context = ex.context
            self.code = ex.code
        elif isinstance(self.message, Exception):
            ex = self.message
            self.message = str(ex)
            self.context = {'trace': tb.format_exception(type(ex), value=ex, tb=ex.__traceback__)}
            self.code = type(ex).__name__
        else:
            self.code = type(self).__name__ if self.code is None else self.code

    def __str__(self):
        return ''.join(self.context.get('trace', [self.message]))


@repo_type(db=False)
@dataclass(frozen=True, slots=True)
class Resource:
    uri: str
    data: Optional[Ref] = None  # -> Datum
    adapter: Optional[str] = None


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
    nodes: list[Ref]  # -> node
    names: Dict[str, Ref]  # -> node
    result: Optional[Ref]  # -> node
    error: Optional[Error]

    def ready(self):
        return (self.result or self.error) is not None


@repo_type(hash=['argv'])
@dataclass
class FnDag(Dag):
    argv: Optional[Ref] = None  # -> node(expr)


@repo_type(db=False)
@dataclass
class Literal:
    value: Ref  # -> datum

    @property
    def error(self):
        pass


@repo_type(db=False)
@dataclass
class Argv(Literal):
    pass


@repo_type(db=False)
@dataclass
class Import:
    dag: Ref  # -> dag | fndag
    node: Optional[Ref] = None  # -> node

    @property
    def value(self):
        ref = self.node or self.dag().result
        return ref().value

    @property
    def error(self):
        ref = self.node or self.dag
        return ref().error


@repo_type(db=False)
@dataclass
class Fn(Import):
    argv: Optional[list[Ref]] = None  # -> node


@repo_type
@dataclass
class Node:
    data: Union[Literal, Argv, Import, Fn]
    doc: Optional[str] = None

    @property
    def value(self):
        return self.data.value

    @property
    def error(self):
        return self.data.error

    @property
    def datum(self):
        return self.value().value


@repo_type
@dataclass
class Datum:
    value: Union[None, str, bool, int, float, Resource, list, dict, set]


@dataclass
class Ctx:
    head: Union[Head, Index]
    commit: Commit
    tree: Tree
    dags: dict
    dag: Optional[Dag]

    @classmethod
    def from_head(cls, ref, dag=None):
        head = asserting(ref())
        commit = head.commit()
        tree = commit.tree()
        dags = tree.dags
        if dag is None and isinstance(head, Index):
            dag = head.dag
        if isinstance(dag, Ref):
            dag = dag()
        return cls(head, commit, tree, dags, dag)


@repo_type(db=False)
@dataclass
class Repo:
    path: str
    user: str = 'unknown'
    head: Ref = field(default_factory=lambda: Ref(DEFAULT_BRANCH))  # -> head
    create: InitVar[bool] = False

    def __post_init__(self, create):
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
                    self.user,
                    self.user,
                    'initial commit',
                )
                self(self.head, Head(self(commit)))
                self('/init', '00000000000000000000000000000000')  # so we all have a common root
            self.checkout(self.head)

    def close(self):
        self.env.close()

    def __enter__(self):
        return self

    def __exit__(self, *errs, **err_kw):
        self.close()

    def __call__(self, key, obj=None, *, return_existing=False) -> Ref:
        return self.put(key, obj, return_existing=return_existing)

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

    def put(self, key, obj=None, *, return_existing=False) -> Ref:
        key, obj = (key, obj) if obj else (obj, key)
        assert obj is not None
        key = key if isinstance(key, Ref) else Ref(key)
        db = key.type if key.to else type(obj).__name__.lower()
        data = packb(obj)
        key2 = key.to or f'{db}/{self.hash(obj)}'
        comp = None
        if key.to is None:
            comp = self._tx[0].get(key2.encode(), db=self.db(db))
            if comp not in [None, data]:
                if return_existing:
                    return Ref(key2)
                msg = f'attempt to update immutable object: {key2}'
                raise AssertionError(msg)
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

    def commits(self, ref=None):
        ref = self.head if ref is None else ref
        return filter(lambda x: x.type == 'commit', self.walk(ref))

    def objects(self, type=None):
        result = set()
        types = list(self.dbs.keys()) if type is None else [type]
        for db in types:
            [result.add(x) for x in self.cursor(db)]
        return result

    def reachable_objects(self):
        result = set()
        for db in ['head', 'index']:
            result = result.union(self.walk(*[k for k in self.cursor(db)]))
        return result

    def unreachable_objects(self):
        return self.objects().difference(self.reachable_objects())

    def gc(self):
        resources = []
        num_deleted = 0
        for ref in self.unreachable_objects():
            obj = ref()
            if isinstance(obj, Datum) and isinstance(obj.value, Resource):
                resources.append(obj.value)
            self.delete(ref)
            num_deleted += 1
        logger.info('deleted %r objects including %r resources', num_deleted, len(resources))
        return resources

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
            author or self.user,
            self.user,
            message or f'merge {c2.id} with {c1.id}',
            created or now()))

    def rebase(self, c1, c2):
        c0 = self.merge_base(c1, c2)

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
                c.parents, c.committer, c.modified = ([x], self.user, now())
                return self(c)
            assert len(p) == 2, f'commit has more than two parents: {commit.to}'
            a, b = (replay(x) for x in p)
            return self.merge(a, b, commit.author, commit.message, commit.created)
        return c2 if c0 == c1 else c1 if c0 == c2 else replay(c2)

    def squash(self, c1, c2):
        c0 = self.merge_base(c1, c2)
        assert c0 == c1, 'cannot squash from non ancestor'
        c = c1()
        c.tree = self.patch(c.tree, self.diff(c.tree, c2().tree))
        c.parents = [c1]
        c.committer = c2().committer
        c.created = now()

        def reparent(commit, old_parent, new_parent):
            comm = commit()
            replaced = [new_parent if x == old_parent else x for x in comm.parents]
            comm.parents = replaced
            ref = self(comm)
            children = self.get_child_commits(commit)
            for child in children:
                reparent(child, commit, ref)
            return ref
        ref = self(c)
        for child in self.get_child_commits(c2):
            reparent(child, c2, ref)
        return ref

    def get_child_commits(self, commit):
        children = set()
        for x in self.reachable_objects():
            if isinstance(x(), Commit) and commit in x().parents:
                children.add(commit)
        return children

    def create_branch(self, branch, ref):
        assert branch.type == 'head', f'unexpected branch type: {branch.type}'
        assert branch() is None, 'branch already exists'
        assert ref.type in ['head', 'commit'], f'unexpected ref type: {ref.type}'
        ref = Head(ref) if ref.type == 'commit' else ref()
        return self(branch, ref)

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

    def extract_nodes(self, obj):
        result = []
        def extract(obj):
            if isinstance(obj, Ref):
                extract(obj())
            elif isinstance(obj, Node):
                if obj not in result:
                    result.append(obj)
            elif isinstance(obj, (list, set, tuple)):
                for x in obj:
                    extract(x)
            elif isinstance(obj, dict):
                for v in obj.values():
                    extract(v)
        return extract(obj) or result

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

    def begin(self, *, message, name=None, dag=None):
        if (name or dag) is None:
            msg = 'either dag or a name is required'
            raise ValueError(msg)
        ctx = Ctx.from_head(self.head)
        if dag is None:
            dag = self(Dag([], {}, None, None))
        ctx.dags[name] = dag
        commit = Commit(
            [ctx.head.commit],
            self(ctx.tree),
            self.user,
            self.user,
            message)
        index = self(Index(self(commit), dag))
        return index

    def put_node(self, data, index: Ref, name=None, doc=None):
        ctx = Ctx.from_head(index)
        node = data if isinstance(data, Ref) else self(Node(data, doc=doc))
        if node not in ctx.dag.nodes:
            ctx.dag.nodes.append(node)
        if name:
            ctx.dag.names[name] = node
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

    def dump_ref(self, ref, recursive=True):
        return to_json([[x, x()] for x in self.walk_ordered(ref)] if recursive else [[ref.to, ref()]])

    def load_ref(self, dump):
        *dump, = (self.put(k, v) for k, v in raise_ex(from_json(dump)))
        return dump[-1] if len(dump) else None

    def start_fn(self, index, *, argv, retry=False, name=None, doc=None):
        fn, *data = map(lambda x: x().datum, argv)
        argv_node = self(Node(Argv(self.put_datum([x().value for x in argv]))))
        fndag = self(FnDag([argv_node], {}, None, None, argv_node), return_existing=True)
        if fndag().error is not None and retry:
            self(fndag, FnDag([argv_node], {}, None, None, argv_node))
        if not fndag().ready():
            uri = urlparse(fn.uri)
            if fn.adapter is None and uri.scheme == 'daggerml':
                result = error = None
                nodes = [argv_node]
                try:
                    result = BUILTIN_FNS[uri.path](*data)
                except Exception as e:
                    error = Error(e)
                if error is None:
                    result = self(Node(Literal(self.put_datum(result))))
                    nodes.append(result)
                self(fndag, FnDag(nodes, {}, result, error, argv_node))
            else:
                cmd = shutil.which(fn.adapter or '')
                assert cmd, f'no such adapter: {fn.adapter}'
                args = [cmd, fn.uri, fn.adapter]
                data = to_json([argv_node.id, self.dump_ref(fndag)])
                proc = subprocess.run(args, input=data, capture_output=True, text=True, check=False)
                err = '' if not proc.stderr else f'\n{proc.stderr}'
                if proc.stderr:
                    logger.error(proc.stderr.rstrip())
                assert proc.returncode == 0, f'{cmd}: exit status: {proc.returncode}{err}'
                self.load_ref(proc.stdout or to_json([]))
        if fndag().ready():
            node = self.put_node(Fn(fndag, None, argv), index=index, name=name, doc=doc)
            raise_ex(node().error)
            return node

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
        ref = self(dag, ctx.dag)
        commit = self.merge(self.head().commit, self(ctx.commit))
        self.set_head(self.head, commit)
        self.delete(index)
        return self.dump_ref(ref)
