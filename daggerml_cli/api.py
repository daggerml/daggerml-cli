from asciidag.graph import Graph as AsciiGraph
from asciidag.node import Node as AsciiNode
import daggerml_cli.config as config
import os
from daggerml_cli.repo import Repo, Ref, Resource
from daggerml_cli.util import asserting
from dataclasses import dataclass
from shutil import rmtree


@dataclass
class Ctx:
    path: str = None
    head: Ref = None

    def __post_init__(self):
        self.path = str(asserting(config.REPO_PATH, 'no repo selected'))
        self.head = Ref('head/%s' % asserting(config.HEAD, 'no branch selected'))


###############################################################################
# REPO ########################################################################
###############################################################################


def current_repo():
    return config.REPO


def repo_path():
    return asserting(config.REPO_PATH, 'no repo selected')


def list_repo():
    return sorted(os.listdir(config.REPO_DIR))


def list_other_repo():
    return sorted([k for k in list_repo() if k != config.REPO])


def create_repo(name):
    path = os.path.join(config.REPO_DIR, name)
    Repo(str(path), create=True)


def delete_repo(name):
    path = os.path.join(config.REPO_DIR, name)
    rmtree(str(path))


def copy_repo(name):
    ctx = Ctx()
    Repo(ctx.path).copy(os.path.join(config.REPO_DIR, name))


def gc_repo():
    ctx = Ctx()
    db = Repo(ctx.path, head=ctx.head)
    with db.tx(True):
        return db.gc()


###############################################################################
# PROJECT #####################################################################
###############################################################################


def init_project(name):
    if name is None:
        os.remove(config.REPO_CONFIG_FILE)
        config.REPO_PATH = None
    else:
        assert name in list_repo(), f'repo not found: {name}'
        os.makedirs(config.PROJECT_DIR, mode=0o700, exist_ok=True)
        with open(config.REPO_CONFIG_FILE, 'w') as f:
            f.write(name+'\n')
        config.REPO_PATH = os.path.join(config.REPO_DIR, name)
    config.REPO = name


###############################################################################
# BRANCH ######################################################################
###############################################################################


def current_branch():
    return config.HEAD


def list_branch():
    ctx = Ctx()
    db = Repo(ctx.path)
    with db.tx():
        return sorted([k.name for k in db.heads()])


def list_other_branch():
    ctx = Ctx()
    return [k for k in list_branch() if k != ctx.head.name]


def create_branch(name):
    ctx = Ctx()
    db = Repo(ctx.path, head=ctx.head)
    with db.tx(True):
        db.create_branch(Ref(f'head/{name}'), ctx.head)
    use_branch(name)


def delete_branch(name):
    ctx = Ctx()
    db = Repo(ctx.path, head=ctx.head)
    with db.tx(True):
        db.delete_branch(Ref(f'head/{name}'))


def use_branch(name):
    if name is None:
        pass
    else:
        assert name in list_branch(), f'branch not found: {name}'
        os.makedirs(config.PROJECT_DIR, mode=0o700, exist_ok=True)
        with open(config.HEAD_CONFIG_FILE, 'w') as f:
            f.write(name+'\n')
    config.HEAD = name


def merge_branch(name):
    ctx = Ctx()
    db = Repo(ctx.path, head=ctx.head)
    with db.tx(True):
        ref = db.merge(ctx.head().commit, Ref(f'head/{name}')().commit)
        db.checkout(db.set_head(ctx.head, ref))
        return ref.name


def rebase_branch(name):
    ctx = Ctx()
    db = Repo(ctx.path, head=ctx.head)
    with db.tx(True):
        ref = db.rebase(Ref(f'head/{name}')().commit, ctx.head().commit)
        db.checkout(db.set_head(ctx.head, ref))
        return ref.name


###############################################################################
# DAG #########################################################################
###############################################################################


def list_dag():
    ctx = Ctx()
    db = Repo(ctx.path, head=ctx.head)
    with db.tx():
        return db.ctx(ctx.head).dags.keys()


def delete_dag(name):
    ctx = Ctx()
    db = Repo(ctx.path, head=ctx.head)
    with db.tx(True):
        c = db.ctx(ctx.head)
        if c.dags.pop(name, None):
            c.commit.tree = db(c.tree)
            print([c.head, db(c.commit)])
            db.set_head(ctx.head, db(c.commit))


###############################################################################
# API #########################################################################
###############################################################################


def invoke_api(token, data):
    try:
        ctx = Ctx()
        db = Repo.new(token) if token else Repo(ctx.path, head=ctx.head)
        op, arg = data

        if op == 'begin':
            with db.tx(True):
                db.begin(arg)
                return {'status': 'ok', 'token': db.state}

        if op == 'put_datum':
            if arg['type'] in ['map', 'list', 'scalar']:
                value = arg['value']
            elif arg['type'] == 'set':
                value = set(arg['value'])
            elif arg['type'] == 'resource':
                value = Resource(arg['value'])
            else:
                raise ValueError(f'unknown datum type: {arg["type"]}')
            with db.tx(True):
                ref = db.put_datum(value)
                return {'status': 'ok', 'result': {'ref': ref.to}}

        if op == 'put_node':
            with db.tx(True):
                ref = db.put_node(arg['type'], arg['expr'], Ref(arg['datum']))
                return {'status': 'ok', 'result': {'ref': ref.to}}

        if op == 'commit':
            res_or_err = Ref(arg['ref']) if arg['ref'] else arg['error']
            with db.tx(True):
                db.commit(res_or_err)
                return {'status': 'ok'}

        raise ValueError(f'no such op: {op}')
    except BaseException as e:
        return {'status': 'error', 'error': e}


###############################################################################
# COMMIT ######################################################################
###############################################################################


def list_commit():
    return []


def commit_log_graph():
    ctx = Ctx()
    db = Repo(ctx.path, head=ctx.head)
    with db.tx():
        def walk_names(x, head=None):
            if x and x[0]:
                k = names[x[0]] if x[0] in names else x[0].name
                tag1 = ' HEAD' if head == db.head.to else ''
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


def revert_commit(commit):
    raise NotImplementedError('not implemented')
