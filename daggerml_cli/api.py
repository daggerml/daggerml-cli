import os
from dataclasses import dataclass
from shutil import rmtree

from asciidag.graph import Graph as AsciiGraph
from asciidag.node import Node as AsciiNode

from daggerml_cli.repo import DEFAULT, Error, FnDag, Literal, Load, Ref, Repo, unroll_datum
from daggerml_cli.util import asserting, makedirs

###############################################################################
# REPO ########################################################################
###############################################################################


def current_repo(config):
    return config.REPO


def repo_path(config):
    return config.REPO_PATH


def list_repo(config):
    if os.path.exists(config.REPO_DIR):
        return sorted(os.listdir(config.REPO_DIR))
    return []


def list_other_repo(config):
    return sorted([k for k in list_repo(config) if k != config.REPO])


def create_repo(config, name):
    config._REPO = name
    Repo(makedirs(config.REPO_PATH), config.USER, create=True)


def use_repo(config, name):
    assert name in list_repo(config), f"no such repo: {name}"
    config.REPO = name


def delete_repo(config, name):
    path = os.path.join(config.REPO_DIR, name)
    rmtree(path)


def copy_repo(config, name):
    Repo(config.REPO_PATH).copy(os.path.join(config.REPO_DIR, name))


def gc_repo(config):
    db = Repo(config.REPO_PATH)
    with db.tx(True):
        return db.gc()


###############################################################################
# PROJECT #####################################################################
###############################################################################


def init_project(config, name, branch=Ref(DEFAULT).name):  # noqa: B008
    if name is not None:
        assert name in list_repo(config), f"repo not found: {name}"
    config.REPO = name
    use_branch(config, branch)


###############################################################################
# BRANCH ######################################################################
###############################################################################


def current_branch(config):
    return config.BRANCH


def list_branch(config):
    if os.path.exists(config.REPO_PATH):
        db = Repo(config.REPO_PATH)
        with db.tx():
            return sorted([k.name for k in db.heads() if k.name])
    return []


def list_other_branch(config):
    return [k for k in list_branch(config) if k != config.BRANCH]


def create_branch(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        db.create_branch(Ref(f"head/{name}"), db.head)
    use_branch(config, name)


def delete_branch(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        db.delete_branch(Ref(f"head/{name}"))


def use_branch(config, name):
    assert name in list_branch(config), f"branch not found: {name}"
    config.BRANCH = name


def merge_branch(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        ref = db.merge(db.head().commit, Ref(f"head/{name}")().commit)
        db.checkout(db.set_head(db.head, ref))
        return ref.name


def rebase_branch(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        ref = db.rebase(Ref(f"head/{name}")().commit, db.head().commit)
        db.checkout(db.set_head(db.head, ref))
        return ref.name


###############################################################################
# DAG #########################################################################
###############################################################################

def get_dag(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(False):
        return db.get_dag(name)


def list_dag(config):
    if os.path.exists(config.REPO_PATH):
        db = Repo(config.REPO_PATH, head=config.BRANCHREF)
        with db.tx():
            return db.ctx(db.head).dags.keys()
    return []


def delete_dag(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        c = db.ctx(db.head)
        if c.dags.pop(name, None):
            c.commit.tree = db(c.tree)
            db.set_head(db.head, db(c.commit))


def begin_dag(config, name, message):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        return db.begin(name=name, message=message)


###############################################################################
# API #########################################################################
###############################################################################


def _invoke_method(f):
    _, fname = f.__name__.split('_', 1)
    _invoke_method.fn_map[fname] = f
    return f
_invoke_method.fn_map = {}

@_invoke_method
def invoke_start_fn(db, index, dag, expr, cache=False, retry=False, path=None):
    with db.tx(True):
        return db.start_fn(index=index, dag=dag, expr=expr, cache=cache, retry=retry, path=path)

@_invoke_method
def invoke_put_literal(db, index, dag, data):
    with db.tx(True):
        return db.put_node(Literal(db.put_datum(data)), index=index, dag=dag)

@_invoke_method
def invoke_put_load(db, index, dag, load_dag):
    with db.tx(True):
        return db.put_node(Load(asserting(db.get_dag(load_dag))), index=index, dag=dag)

@_invoke_method
def invoke_commit(db, index, dag, result, **kw):
    with db.tx(True):
        return db.commit(res_or_err=result, index=index, dag=dag, **kw)

@_invoke_method
def invoke_get_node_value(db, _, node: Ref):
    with db.tx():
        return db.get_node_value(node)

@_invoke_method
def invoke_get_expr(db, _, dag):
    with db.tx():
        return [unroll_datum(x().value) for x in dag().expr]

@_invoke_method
def invoke_get_fn_meta(db, _, fndag):
    with db.tx():
        assert isinstance(fndag(), FnDag)
        return db.get_fn_meta(fndag)

@_invoke_method
def invoke_update_fn_meta(db, _, fndag, old_meta: str, new_meta: str):
    with db.tx(True):
        return db.update_fn_meta(fndag, old_meta, new_meta)


def invoke_api(config, token, data):
    db = None

    def no_such_op(name):
        def inner(*_args, **_kwargs):
            raise ValueError(f"no such op: {name}")
        return inner

    try:
        # db = token if token else Repo(config.REPO_PATH, config.USER, config.BRANCHREF)
        db = Repo(config.REPO_PATH, config.USER, config.BRANCHREF)
        op, args, kwargs = data
        return _invoke_method.fn_map.get(op, no_such_op(op))(db, token, *args, **kwargs)
    except Exception as e:
        raise Error.from_ex(e) from e


###############################################################################
# COMMIT ######################################################################
###############################################################################


def list_commit(config):
    return []


def commit_log_graph(config):
    @dataclass
    class GNode:
        commit: Ref
        parents: list[Ref]
        children: list[Ref]

    db = Repo(config.REPO_PATH, config.USER, head=config.BRANCHREF)

    with db.tx():
        def walk_names(x, head=None):
            if x and x[0]:
                k = names[x[0]] if x[0] in names else x[0].name
                tag1 = " HEAD" if head and head.to == db.head.to else ""
                tag2 = f" {head.name}" if head else ""
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
