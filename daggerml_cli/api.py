import daggerml_cli.config as config
import os
from daggerml_cli.repo import Repo, Ref, Resource
from daggerml_cli.util import asserting
from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree


@dataclass
class Ctx:
    path: str = None
    head: Ref = None

    def __post_init__(self):
        self.path = str(asserting(config.REPO_PATH, 'no repo selected'))
        self.head = Ref('head/%s' % asserting(config.HEAD, 'no branch selected'))


def current_repo():
    return config.REPO


def list_repo():
    return sorted(os.listdir(config.REPO_DIR))


def create_repo(name):
    path = Path.joinpath(config.REPO_DIR, name)
    Repo(str(path), create=True)
    use_repo(name)


def delete_repo(name):
    path = Path.joinpath(config.REPO_DIR, name)
    rmtree(str(path))
    if current_repo() == name:
        use_repo(None)


def use_repo(name):
    if name is None:
        os.remove(config.REPO_CONFIG_FILE)
        config.REPO_PATH = None
    else:
        assert name in list_repo(), f'repo not found: {name}'
        os.makedirs(config.CONFIG_DIR, mode=0o700, exist_ok=True)
        with open(config.REPO_CONFIG_FILE, 'w') as f:
            f.write(name+'\n')
        config.REPO_PATH = Path.joinpath(config.REPO_DIR, name)
    config.REPO = name


def current_branch():
    return config.HEAD


def list_branch():
    ctx = Ctx()
    db = Repo(ctx.path)
    with db.tx():
        return sorted(['/'.join(k.split('/')[1:]) for k in db.heads()])


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
        os.makedirs(config.CONFIG_DIR, mode=0o700, exist_ok=True)
        with open(config.HEAD_CONFIG_FILE, 'w') as f:
            f.write(name+'\n')
    config.HEAD = name


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
