import json
import logging
import os
import subprocess
from contextlib import contextmanager
from copy import copy
from dataclasses import fields, is_dataclass
from shutil import rmtree
from typing import TYPE_CHECKING, Union

import jmespath
from asciidag.graph import Graph as AsciiGraph
from asciidag.node import Node as AsciiNode

from daggerml_cli.db import Cache
from daggerml_cli.repo import (
    BUILTIN_FNS,
    DEFAULT_BRANCH,
    CheckedRef,
    Ctx,
    Dag,
    Error,
    FnDag,
    Import,
    Index,
    Literal,
    Node,
    Ref,
    Repo,
    Resource,
    unroll_datum,
)
from daggerml_cli.topology import node_info, topology
from daggerml_cli.util import asserting, detect_executable, flatten, makedirs, some

if TYPE_CHECKING:
    from daggerml_cli.config import Config

###############################################################################
# HELPERS #####################################################################
###############################################################################

log = logging.getLogger(__name__)


def jsdata(x, full_id=True):
    if isinstance(x, Ref):
        _data = getattr(x, "_data", None)
        id = x.to if full_id else x.id
        if _data is None:
            return id
        x = {"id": id, **{k: getattr(x, k) for k in _data}}
    if isinstance(x, (tuple, list, set)):
        return [jsdata(y, full_id=full_id) for y in x]
    if isinstance(x, dict):
        return {k: jsdata(v, full_id=full_id) for k, v in x.items()}
    if is_dataclass(x):
        return jsdata(x.__dict__, full_id=full_id)
    return x


def with_query(f, query):
    return lambda *args, **kwargs: jmespath.search(query, jsdata(f(*args, **kwargs)))


def with_attrs(x, **kwargs):
    x = copy(x)
    y = x()
    kwargs.update({field.name: getattr(y, field.name) for field in fields(y)})
    for k, v in kwargs.items():
        object.__setattr__(x, k, v)
    object.__setattr__(x, "_data", sorted(kwargs.keys()))
    return x


###############################################################################
# REPO ########################################################################
###############################################################################


def repo_path(config):
    return config.REPO_PATH


def list_repo(config):
    if os.path.exists(config.REPO_DIR):
        xs = sorted(os.listdir(config.REPO_DIR))
        return [{"name": x, "path": os.path.join(config.REPO_DIR, x)} for x in xs]
    return []


def list_other_repo(config):
    return [k for k in list_repo(config) if k["name"] != config.REPO]


def create_repo(config, name):
    config._REPO = name
    with Repo(makedirs(config.REPO_PATH), user=config.USER, create=True):
        pass


def resize_repo(config, name, newsize):
    config._REPO = name
    with open(os.path.join(config.REPO_PATH, "config"), "w") as f:
        json.dump({"map_size": newsize}, f)


def delete_repo(config, name):
    path = os.path.join(config.REPO_DIR, name)
    rmtree(path)


def copy_repo(config, name):
    with Repo(config.REPO_PATH) as db:
        db.copy(os.path.join(config.REPO_DIR, name))


def gc_repo(config):
    with Repo(config.REPO_PATH) as db:
        with db.tx(True):
            return db.gc()


def list_deleted(config):
    with Repo(config.REPO_PATH) as db:
        with db.tx():
            return [{"id": x, **x().__dict__} for x in db.objects("deleted")]


def remove_deleted(config, ref):
    with Repo(config.REPO_PATH) as db:
        with db.tx(True):
            assert ref.type == "deleted"
            db.delete(ref)


###############################################################################
# REF #########################################################################
###############################################################################


def dump_ref(config, ref, recursive=True):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            return db.dump_ref(ref, recursive)


def load_ref(config, ref):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            return db.load_ref(ref)


###############################################################################
# STATUS ######################################################################
###############################################################################


def status(config):
    return {
        "repo": config.get("REPO"),
        "branch": config.get("BRANCH"),
        "user": config.get("USER"),
        "config_dir": config.get("CONFIG_DIR"),
        "project_dir": config.get("PROJECT_DIR") and os.path.abspath(config.get("PROJECT_DIR")),
        "cache_path": config.get("CACHE_PATH"),
    }


###############################################################################
# CONFIG ######################################################################
###############################################################################


def config_repo(config, name):
    assert name in with_query(list_repo, "[*].name")(config), f"no such repo: {name}"
    config.REPO = name
    config_branch(config, Ref(DEFAULT_BRANCH).id)


def config_branch(config, name):
    assert f"head/{name}" in jsdata(list_branch(config)), f"no such branch: {name}"
    config.BRANCH = name


def config_user(config, user):
    config.USER = user


def config_cache(config, path):
    config.CACHE_PATH = path


###############################################################################
# BRANCH ######################################################################
###############################################################################


def current_branch(config):
    return config.BRANCH


def list_branch(config):
    with Repo(config.REPO_PATH) as db:
        with db.tx():
            return sorted([x for x in db.heads()], key=lambda y: y.id)


def list_other_branch(config):
    return [k for k in list_branch(config) if k.id != config.BRANCH]


def create_branch(config, name, commit=None):
    kw = {}
    if commit is not None:
        kw["head"] = config.BRANCHREF
    with Repo(config.REPO_PATH, **kw) as db:
        with db.tx(True):
            ref = db.head if commit is None else Ref(f"commit/{commit}")
            db.create_branch(Ref(f"head/{name}"), ref)
    config_branch(config, name)


def delete_branch(config, name):
    with Repo(config.REPO_PATH) as db:
        with db.tx(True):
            db.delete_branch(Ref(f"head/{name}"))


def merge_branch(config, name):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            ref = db.merge(db.head().commit, Ref(f"head/{name}")().commit)
            db.checkout(db.set_head(db.head, ref))
        return ref.id


def rebase_branch(config, name):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            ref = db.rebase(Ref(f"head/{name}")().commit, db.head().commit)
            db.checkout(db.set_head(db.head, ref))
        return ref.id


###############################################################################
# DAG #########################################################################
###############################################################################


def list_dags(config, *, all=False):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            dags = Ctx.from_head(db.head).dags
            result = [with_attrs(v, name=k) for k, v in dags.items()]
            if all:
                dag_ids = [d.to for d in dags.values()]
                for obj in db.walk(db.head):
                    if isinstance(obj(), Dag) and obj.to not in dag_ids:
                        result.append(with_attrs(obj, name=None))
            return result


def delete_dag(config, name, message):
    with Repo(config.REPO_PATH, user=config.USER, head=config.BRANCHREF) as db:
        with db.tx(True):
            return db.delete_dag(name, message)


def begin_dag(config, *, name=None, message, dump=None):
    with Repo(config.REPO_PATH, user=config.USER, head=config.BRANCHREF) as db:
        with db.tx(True):
            return db.begin(name=name, message=message, dump=dump)


def get_dag(config, name_or_id, db=None):
    if db is None:
        with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
            with db.tx():
                return get_dag(None, name_or_id, db=db)
    if len([x for x in list(name_or_id) if x == "/"]) == 1:
        ref = Ref(name_or_id)
        if ref() is not None:
            return ref
    return Ctx.from_head(db.head).dags.get(name_or_id)


def describe_dag(config, ref):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            assert isinstance(ref(), Dag)
            desc = topology(db, ref)
            return desc


###############################################################################
# CACHE #######################################################################
###############################################################################


def create_cache(config):
    with Cache(config.CACHE_PATH, create=True):
        pass


def delete_cache(config, cache_key: Ref = None):
    with Cache(config.CACHE_PATH) as cache:
        return cache.delete(cache_key)


def list_cache(config):
    # walk all indexes and return all FnCache objects
    with Cache(config.CACHE_PATH) as cache:
        return list(cache)


def info_cache(config, cache_key: Ref = None):
    with Cache(config.CACHE_PATH) as cache:
        val = json.loads(cache.get(cache_key))[-1][1][1]
        return {"cache_key": cache_key, "dag_id": val}


def put_cache(config: "Config", dag: Ref):
    with Cache(config.CACHE_PATH) as cache:
        dump = dump_ref(config, dag, recursive=True)
        assert isinstance(dump, str), "dump_ref should return a JSON string"
        cache_key = describe_dag(config, dag)["cache_key"]
        if cache_key is None:
            raise ValueError("dag has no cache key")
        cache.put(cache_key, dump, old_value=None)
    return cache_key


###############################################################################
# INDEX #######################################################################
###############################################################################


def list_indexes(config):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            return [
                with_attrs(
                    x,
                    created=x().commit().created,
                    modified=x().commit().modified,
                    message=x().commit().message,
                    author=x().commit().author,
                )
                for x in db.indexes()
            ]


def delete_index(config, index: Ref):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            assert isinstance(index(), Index), f"no such index: {index.id}"
            db.delete(index)
    return True


###############################################################################
# Node ########################################################################
###############################################################################


def describe_node(config: "Config", node: Ref):
    with Repo.from_config(config) as db:
        with db.tx():
            return node_info(node)


def backtrack_node(config: "Config", node: Ref, *keys: Union[str, int]) -> Ref:
    with Repo.from_config(config) as db:
        with db.tx():
            keys = list(keys)
            while len(keys) > 0:
                key = keys.pop(0)
                if not isinstance(node, Ref) or node.type != "node":
                    raise TypeError(f"invalid type {node!r} for backtrack_node")
                argv = node().data.argv
                fn, *args = (x().value().value for x in argv)
                if fn.uri == "daggerml:list":
                    key = int(key)
                elif fn.uri == "daggerml:dict":
                    i = args.index(key)
                    key = i + 1
                else:
                    raise Error(f"{fn.uri} is not a collection constructor")
                node = argv[key + 1]
            return node


###############################################################################
# API #########################################################################
###############################################################################


def deref(config, ref):
    """
    Dereference a Ref object to its underlying data.
    """
    with Repo.from_config(config) as db:
        with db.tx():
            return db.get(ref)


def invoke_op(f):
    _, fname = f.__name__.split("_", 1)
    if not hasattr(invoke_op, "fns"):
        invoke_op.fns = {}
    invoke_op.fns[fname] = f
    return f


def format_ops():
    return ", ".join(sorted([*list(invoke_op.fns.keys()), *BUILTIN_FNS.keys()]))


@invoke_op
def op_start_fn(db, index, argv, name=None, doc=None):
    with db.tx(True):
        argv = [op_put_literal(db, index, x) for x in argv]
        return db.start_fn(index, argv=argv, name=name, doc=doc)


@invoke_op
def op_put_literal(db, index, data, name=None, doc=None):
    # TODO: refactor so that Resource.data -> Ref(datum)
    def fn(args):
        fn_ = None
        if isinstance(args, list):
            args = [fn(x) for x in args]
            if any(isinstance(x, Ref) for x in args):
                fn_ = db.put_datum(Resource("daggerml:list"))
        elif isinstance(args, dict):
            args = {k: fn(v) for k, v in args.items()}
            if any(isinstance(x, Ref) for x in args.values()):
                fn_ = db.put_datum(Resource("daggerml:dict"))
                args = flatten(args.items())
        elif isinstance(args, set):
            args = {fn(x) for x in args}
            if any(isinstance(x, Ref) for x in args):
                fn_ = db.put_datum(Resource("daggerml:set"))
        if fn_ is not None:
            return op_start_fn(db, index, [fn_, *args])
        return args

    with db.tx(True):
        data = fn(data)
        if isinstance(data, Ref) and data.type == "node":
            return op_set_node(db, index, name, data) if name else data
        result = db.put_node(Literal(db.put_datum(data)), index=index, name=name, doc=doc)
        return result


@invoke_op
def op_put_load(db, index, dag, node=None, name=None, doc=None):
    with db.tx(True):
        dag = op_get_dag(db, index, dag) if isinstance(dag, str) else dag
        return db.put_node(Import(dag, node), index=index, name=name, doc=doc)


@invoke_op
def op_commit(db, index, result):
    with db.tx(True):
        return db.commit(res_or_err=result, index=index)


@invoke_op
def op_get_dag(db, index, name, recurse=False):
    with db.tx():
        if isinstance(name, str):
            ref = get_dag(None, name, db=db)
            assert ref, f"no such dag: {name}"
            return ref
        if recurse:
            name = name().data.node
        return name().data.dag


@invoke_op
def op_get_names(db, index, dag: Ref = None):
    with db.tx():
        dag = dag or index().dag
        return dag().names


@invoke_op
def op_get_node(db, index, name, dag: Ref = None):
    with db.tx():
        dag = dag or index().dag
        nodes = [x for x in dag().nodes if x.to == name]
        if len(nodes) > 0:
            return nodes[0]
        if name not in dag().names:
            raise KeyError(f"Key {name} not in {sorted(dag().names)}")
        return dag().names[name]


@invoke_op
def op_set_node(db, index, name, node: Ref):
    with db.tx(True):
        return db.put_node(node, index, name=name)


@invoke_op
def op_get_node_value(db, _, node: Ref):
    with db.tx():
        return db.get_node_value(node)


@invoke_op
def op_get_argv(db, index, dag: Ref = None):
    with db.tx():
        dag = dag() if dag else index().dag()
        if isinstance(dag, FnDag):
            return dag.argv
        if isinstance(dag, Node):
            return dag.data.argv
        raise TypeError(f"cannot get argv from {dag.type} dag: {dag.id}")


@invoke_op
def op_get_result(db, index, dag: Ref = None):
    with db.tx():
        dag = dag() if dag else index().dag()
        return dag.result or dag.error


@invoke_op
def op_unroll(db, index, node):
    with db.tx():
        return unroll_datum(node().value())


def invoke_api(config, token, data):
    def no_such_op(name):
        def inner(*_args, **_kwargs):
            raise ValueError(f"no such op: {name}")

        return inner

    tok_to = getattr(token, "to", "NONE")
    index = CheckedRef(tok_to, Index, f"invalid token: {tok_to}")
    try:
        with Repo.from_config(config) as db:
            op, args, kwargs = data
            if op in BUILTIN_FNS:
                with db.tx(True):
                    fn = db.put_datum(Resource(f"daggerml:{op}"))
                    fn = op_put_literal(db, index, fn, name=f"daggerml:{op}")
                return op_start_fn(db, index, [fn, *args], **kwargs)
            return invoke_op.fns.get(op, no_such_op(op))(db, index, *args, **kwargs)
    except Exception as e:
        raise Error(e) from e


###############################################################################
# COMMIT ######################################################################
###############################################################################


def list_commit(config):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            result = [with_attrs(x) for x in db.commits()]
            return sorted(result, key=lambda x: x.modified, reverse=True)


def describe_commit(config, commit: Ref = None):
    kw = {}
    if commit is None:
        kw["head"] = config.BRANCHREF
    with Repo(config.REPO_PATH, **kw) as db:
        with db.tx():
            commit = commit or db.head().commit
            return with_attrs(commit, dags=commit().tree().dags)


def commit_log_graph(config, output="ascii"):
    with Repo(config.REPO_PATH, user=config.USER) as db:
        with db.tx():

            def walk_names(x, head=None):
                if x and x[0]:
                    k = names[x[0]] if x[0] in names else x[0].to
                    tag1 = " HEAD" if head and head.to == db.head.to else ""
                    tag2 = f" {head.id}" if head else ""
                    names[x[0]] = f"{k}{tag1}{tag2}"
                    attrs[x[0]] = {"head": head.id if head else None}
                    [walk_names(p) for p in x[1]]

            def walk_nodes(x):
                if x and x[0]:
                    if x[0] not in nodes:
                        parents = [walk_nodes(y) for y in x[1] if y]
                        if output == "ascii":
                            nodes[x[0]] = AsciiNode(names[x[0]], parents=parents)
                        else:
                            nodes[x[0]] = with_attrs(x[0], head=attrs[x[0]]["head"])
                    return nodes[x[0]]

            names = {}
            nodes = {}
            attrs = {}
            log = dict(asserting(db.log("head")))
            ks = [db.head, *[k for k in log.keys() if k != db.head]]
            [walk_names(log[k], head=k) for k in ks]
            heads = [walk_nodes(log[k]) for k in ks]
            if output == "ascii":
                AsciiGraph().show_nodes(heads)
                return
            out = [*heads, *[x for x in reversed(nodes.values()) if x not in heads]]
            return out
            # return print(json.dumps([asdict(x) for x in out], indent=2, sort_keys=True))


def revert_commit(config, commit):
    raise NotImplementedError("not implemented")


###############################################################################
# UTIL ########################################################################
###############################################################################


@contextmanager
def chdir(path):
    old_path = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(old_path)


def reproducible_tar(dir_, file, *exclude):
    # https://h2.jaguarpaw.co.uk/posts/reproducible-tar/
    # https://www.gnu.org/software/tar/manual/html_section/Reproducibility.html
    # https://reproducible-builds.org/docs/archives/
    tar = some([detect_executable(x, "\\bGNU\\b") for x in ["tar", "gtar"]])
    assert tar, "GNU tar not found on PATH: tried 'tar' and 'gtar'"
    with chdir(dir_):
        proc = subprocess.run(
            [
                tar,
                "--format=posix",
                "--pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime,delete=mtime",
                "--mtime=1970-01-01 00:00:00Z",
                "--sort=name",
                "--numeric-owner",
                "--owner=0",
                "--group=0",
                *[f"--exclude={x}" for x in exclude],
                "-cvf",
                file,
                ".",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    err = "" if not proc.stderr else f"\n{proc.stderr}"
    assert proc.returncode == 0, f"{tar}: exit status: {proc.returncode}{err}"
