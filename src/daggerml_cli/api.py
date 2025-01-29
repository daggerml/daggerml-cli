import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass, is_dataclass
from pathlib import Path
from shutil import rmtree

import jmespath
from asciidag.graph import Graph as AsciiGraph
from asciidag.node import Node as AsciiNode

from daggerml_cli.repo import (
    BUILTIN_FNS,
    DEFAULT_BRANCH,
    CheckedRef,
    Ctx,
    Dag,
    Error,
    Import,
    Index,
    Literal,
    Node,
    Ref,
    Repo,
    Resource,
    unroll_datum,
)
from daggerml_cli.topology import topology
from daggerml_cli.util import asserting, makedirs

###############################################################################
# HELPERS #####################################################################
###############################################################################

logger = logging.getLogger(__name__)


def jsdata(x):
    if isinstance(x, (tuple, list, set)):
        return [jsdata(y) for y in x]
    if isinstance(x, dict):
        return {k: jsdata(v) for k, v in x.items()}
    if isinstance(x, Ref):
        return x.id
    if is_dataclass(x):
        return jsdata(x.__dict__)
    return x


def with_query(f, query):
    return lambda *args, **kwargs: jmespath.search(query, jsdata(f(*args, **kwargs)))


def with_attrs(x, **kwargs):
    y = x()
    for k, v in kwargs.items():
        setattr(y, k, v)
    return y


@contextmanager
def tx(config, write=False):
    with Repo(config.REPO_PATH, head=config.BRANCHREF, user=config.USER) as db:
        with db.tx(write):
            yield db


###############################################################################
# REPO ########################################################################
###############################################################################


def repo_path(config):
    return config.REPO_PATH


def list_repo(config):
    if os.path.exists(config.REPO_DIR):
        xs = sorted(os.listdir(config.REPO_DIR))
        return [{'name': x, 'path': os.path.join(config.REPO_DIR, x)} for x in xs]
    return []


def list_other_repo(config):
    return [k for k in list_repo(config) if k['name'] != config.REPO]


def create_repo(config, name):
    config._REPO = name
    with Repo(makedirs(config.REPO_PATH), config.USER, create=True):
        pass


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
        'repo': config.get('REPO'),
        'branch': config.get('BRANCH'),
        'user': config.get('USER'),
        'config_dir': config.get('CONFIG_DIR'),
        'project_dir': config.get('PROJECT_DIR') and os.path.abspath(config.get('PROJECT_DIR')),
    }


###############################################################################
# CONFIG ######################################################################
###############################################################################


def config_repo(config, name):
    assert name in with_query(list_repo, '[*].name')(config), f"no such repo: {name}"
    config.REPO = name
    config_branch(config, Ref(DEFAULT_BRANCH).id)


def config_branch(config, name):
    assert name in jsdata(list_branch(config)), f"no such branch: {name}"
    config.BRANCH = name


def config_user(config, user):
    config.USER = user


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
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            ref = db.head if commit is None else Ref(f'commit/{commit}')
            db.create_branch(Ref(f"head/{name}"), ref)
    config_branch(config, name)


def delete_branch(config, name):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
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


def list_dags(config):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            dags = Ctx.from_head(db.head).dags
            result = [with_attrs(v, id=v, name=k) for k, v in dags.items()]
            return sorted(result, key=lambda x: x.name)


def begin_dag(config, *, name=None, message, dump=None):
    with Repo(config.REPO_PATH, config.USER, head=config.BRANCHREF) as db:
        with db.tx(True):
            dag = None if dump is None else db.load_ref(dump)
            return db.begin(name=name, message=message, dag=dag)


def describe_dag(config, ref):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            assert isinstance(ref(), Dag)
            return topology(ref)


def write_dag_html(config, ref):
    with open(Path(__file__).parent / "dag-viz.html") as f:
        html = f.read()
    return html.replace('"REPLACEMENT_TEXT"', json.dumps(jsdata(describe_dag(config, ref)), indent=2))


###############################################################################
# INDEX #######################################################################
###############################################################################


def list_indexes(config):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            return [with_attrs(x, id=x) for x in db.indexes()]


def delete_index(config, index: Ref):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            assert isinstance(index(), Index), f'no such index: {index.id}'
            db.delete(index)
    return True


###############################################################################
# API #########################################################################
###############################################################################


def invoke_op(f):
    _, fname = f.__name__.split('_', 1)
    if not hasattr(invoke_op, 'fns'):
        invoke_op.fns = {}
    invoke_op.fns[fname] = f
    return f


def format_ops():
    return ', '.join(sorted([*list(invoke_op.fns.keys()), *BUILTIN_FNS.keys()]))


@invoke_op
def op_start_fn(db, index, argv, retry=False, name=None, doc=None):
    with db.tx(True):
        return db.start_fn(index, argv=argv, retry=retry, name=name, doc=doc)


@invoke_op
def op_put_literal(db, index, data, name=None, doc=None):
    with db.tx(True):
        if isinstance(data, Ref) and isinstance(data(), Node):
            return op_set_node(db, index, name, data) if name else data
        nodes = db.extract_nodes(data)
        result = Literal(db.put_datum(data))
        if not len(nodes):
            return db.put_node(result, index=index, name=name, doc=doc)
        else:
            fn = Literal(db.put_datum(Resource('daggerml:build')))
            fn = db.put_node(fn, index=index, name='daggerml:build')
            result = db.put_node(result, index=index)
            nodes = [db.put_node(x.data, index=index, doc=x.doc) for x in nodes]
            result = db.start_fn(index, argv=[fn, result, *nodes], name=name, doc=doc)
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
def op_get_dag(db, index, name=None):
    with db.tx():
        if isinstance(name, str):
            ref = db.get_dag(name)
            assert ref, f'no such dag: {name}'
            return ref
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
        return dag.argv


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
    index = CheckedRef(getattr(token, 'to', 'NONE'), Index, 'invalid token')
    try:
        with Repo(config.REPO_PATH, config.USER, config.BRANCHREF) as db:
            op, args, kwargs = data
            if op in BUILTIN_FNS:
                with db.tx(True):
                    fn = db.put_datum(Resource(f'daggerml:{op}'))
                    fn = op_put_literal(db, index, fn, name=f'daggerml:{op}')
                    argv = [fn, *[op_put_literal(db, index, x) for x in args]]
                return op_start_fn(db, index, argv, **kwargs)
            return invoke_op.fns.get(op, no_such_op(op))(db, index, *args, **kwargs)
    except Exception as e:
        raise Error(e) from e


###############################################################################
# COMMIT ######################################################################
###############################################################################


def list_commit(config):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            result = [with_attrs(x, id=x) for x in db.commits()]
            return sorted(result, key=lambda x: x.modified, reverse=True)


def commit_log_graph(config):
    @dataclass
    class GNode:
        commit: Ref
        parents: list[Ref]
        children: list[Ref]

    with Repo(config.REPO_PATH, config.USER, head=config.BRANCHREF) as db:
        with db.tx():
            def walk_names(x, head=None):
                if x and x[0]:
                    k = names[x[0]] if x[0] in names else x[0].id
                    tag1 = " HEAD" if head and head.to == db.head.to else ""
                    tag2 = f" {head.id}" if head else ""
                    names[x[0]] = f"{k}{tag1}{tag2}"
                    [walk_names(p) for p in x[1]]

            def walk_nodes(x):
                if x and x[0]:
                    if x[0] not in nodes:
                        parents = [walk_nodes(y) for y in x[1] if y]
                        nodes[x[0]] = AsciiNode(names[x[0]], parents=parents)
                    return nodes[x[0]]
            names = {}
            nodes = {}
            log = dict(asserting(db.log("head")))
            ks = [db.head, *[k for k in log.keys() if k != db.head]]
            [walk_names(log[k], head=k) for k in ks]
            heads = [walk_nodes(log[k]) for k in ks]
            AsciiGraph().show_nodes(heads)


def revert_commit(config, commit):
    raise NotImplementedError("not implemented")
