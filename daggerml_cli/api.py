import os
from dataclasses import dataclass
from shutil import rmtree

from asciidag.graph import Graph as AsciiGraph
from asciidag.node import Node as AsciiNode

from daggerml_cli.repo import DEFAULT, Error, Fn, Literal, Load, Node, Ref, Repo, Resource, from_data, to_data
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
    with config:
        config._REPO = name
        Repo(makedirs(config.REPO_PATH), config.USER, create=True)


def use_repo(config, name):
    assert name in list_repo(config), f'no such repo: {name}'
    with config:
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
    with config:
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
    with config:
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


def put_node(db, type, data):
    if type == 'literal':
        return db.put_node(Node(Literal(db.put_datum(from_data(data)))))
    if type == 'load':
        return db.put_node(Node(Load(db.get_dag(data))))
    if type == 'fn':
        data['expr'] = [Ref(x) for x in data['expr']]
        if 'replace' in data:
            data['replace'] = Fn([x().value for x in data['expr']], Ref(data['replace']))
        if 'error' in data:
            data['error'] = from_data(data['error'])
        if 'value' in data:
            data['value'] = db.put_datum(from_data(data['value']))
        try:
            fn = db.put_fn(**data)
        except Error as e:
            context = {
                'info': e.context['new_fn'].info,
                'has_value': e.context['new_fn'].value is not None,
                'has_error': e.context['new_fn'].error is not None,
                'replace': e.context['new_fn'].fnex.to,
            }
            raise Error(e.message, context=context) from None
        if fn.value or fn.error:
            return db.put_node(Node(fn))
        return fn
    raise Error('unknown node type')


def unroll_datum(val):
    val = val.value
    if isinstance(val, (bool, int, float, str, Resource)):
        return val
    if isinstance(val, list):
        return [unroll_datum(x()) for x in val]
    if isinstance(val, set):
        return {unroll_datum(x()) for x in val}
    if isinstance(val, dict):
        return {k: unroll_datum(v()) for k, v in val.items()}
    raise RuntimeError(f'unknown type: {type(val)}')


def invoke_api(config, token, data):
    try:
        db = Repo.from_state(token) if token else Repo(config.REPO_PATH, config.USER, head=config.BRANCHREF)
        op, *arg = data

        if op == 'begin':
            name, user, message = arg
            with db.tx(True):
                db.begin(name, message)
                return {'status': 'ok', 'token': db.state}

        if op == 'put_node':
            type_, data = arg
            with db.tx(True):
                ref = put_node(db, type_, data)
                result = {'ref': ref.to} if isinstance(ref, Ref) else {'info': ref.info, 'replace': ref.fnex.to}
                return {'status': 'ok', 'result': result, 'token': db.state}

        if op == 'get_node':
            node_id, = arg
            with db.tx():
                ref = Ref(node_id)
                node = ref().node
                value = node.value() if node.value is not None else None
                d = {'node_id': ref.to, 'value': value}
                if hasattr(node, 'info'):
                    d['info'] = node.info
                if hasattr(node, 'error'):
                    d['error'] = node.error
                return {
                    'status': 'ok',
                    'result': to_data(d),
                    'token': db.state
                }

        if op == 'commit':
            arg, = arg
            res_or_err = Ref(arg['ref']) if arg.get('ref') else arg['error']
            with db.tx(True):
                db.commit(res_or_err)
                return {'status': 'ok'}

        raise ValueError(f'no such op: {op}')
    except Error as e:
        return {'status': 'error', 'error': {'code': type(e).__name__, 'message': str(e), 'context': to_data(e.context)}}
    except Exception as e:
        return {'status': 'error', 'error': {'code': type(e).__name__, 'message': str(e)}}


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
