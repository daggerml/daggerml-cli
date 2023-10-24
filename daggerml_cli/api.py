import os
from dataclasses import dataclass
from shutil import rmtree

from asciidag.graph import Graph as AsciiGraph
from asciidag.node import Node as AsciiNode

from daggerml_cli.repo import DEFAULT, Error, Literal, Load, Node, Ref, Repo
from daggerml_cli.util import makedirs

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
    return sorted([k for k in list_repo() if k != config.REPO])


def create_repo(config, name):
    config._REPO = name
    Repo(makedirs(config.REPO_PATH), config.USER, create=True)


def use_repo(config, name):
    assert name in list_repo(config), f'no such repo: {name}'
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


def init_project(config, name, branch=Ref(DEFAULT).name):
    if name is not None:
        assert name in list_repo(config), f'repo not found: {name}'
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
            return sorted([k.name for k in db.heads()])
    return []


def list_other_branch(config):
    return [k for k in list_branch(config) if k != config.BRANCH]


def create_branch(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        db.create_branch(Ref(f'head/{name}'), db.head)
    use_branch(config, name)


def delete_branch(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        db.delete_branch(Ref(f'head/{name}'))


def use_branch(config, name):
    assert name in list_branch(config), f'branch not found: {name}'
    config.BRANCH = name


def merge_branch(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        ref = db.merge(db.head().commit, Ref(f'head/{name}')().commit)
        db.checkout(db.set_head(db.head, ref))
        return ref.name


def rebase_branch(config, name):
    db = Repo(config.REPO_PATH, head=config.BRANCHREF)
    with db.tx(True):
        ref = db.rebase(Ref(f'head/{name}')().commit, db.head().commit)
        db.checkout(db.set_head(db.head, ref))
        return ref.name


###############################################################################
# DAG #########################################################################
###############################################################################


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


###############################################################################
# API #########################################################################
###############################################################################


def invoke_api(config, token, data):
    db = None
    api = {}

    def api_method(f):
        api[f.__name__] = f
        return f

    def no_such_op(name):
        def inner(*args, **kwargs):
            raise ValueError(f'no such op: {name}')
        return inner

    @api_method
    def begin(name, message):
        with db.tx(True):
            db.begin(name, message)
            return db.state

    @api_method
    def put_literal(data):
        with db.tx(True):
            return db.put_node(Node(Literal(db.put_datum(data))))

    @api_method
    def put_load(dag):
        with db.tx(True):
            return db.put_node(Node(Load(db.get_dag(dag))))

    @api_method
    def put_fn(expr, info=None, value=None, error=None, replacing=None):
        with db.tx(True):
            if value is not None:
                value = db.put_datum(value)
            fn = db.put_fn(expr, info, value, error, replacing)
            return db.put_node(Node(fn)) if fn.value or fn.error else fn

    @api_method
    def commit(result):
        with db.tx(True):
            db.commit(result)

    @api_method
    def get_node(ref):
        with db.tx():
            return ref()

    try:
        db = Repo.from_state(token) if token else Repo(config.REPO_PATH, config.USER, config.BRANCHREF)
        op, args, kwargs = data
        return api.get(op, no_such_op(op))(*args, **kwargs)
    except Exception as e:
        raise Error.from_ex(e)


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
                tag1 = ' HEAD' if head and head.to == db.head.to else ''
                tag2 = f' {head.name}' if head else ''
                names[x[0]] = f'{k}{tag1}{tag2}'
                [walk_names(p) for p in x[1]]

        def walk_nodes(x):
            if x and x[0]:
                if x[0] not in nodes:
                    parents = [walk_nodes(y) for y in x[1] if y]
                    nodes[x[0]] = AsciiNode(names[x[0]], parents=parents)
                return nodes[x[0]]

        names = {}
        nodes = {}
        log = db.log('head')
        ks = [db.head, *[k for k in log.keys() if k != db.head]]
        [walk_names(log[k], head=k) for k in ks]
        heads = [walk_nodes(log[k]) for k in ks]
        AsciiGraph().show_nodes(heads)


def revert_commit(config, commit):
    raise NotImplementedError('not implemented')
