import daggerml_cli.api as api
import daggerml_cli.config as config
import os
from daggerml_cli.repo import Repo, Ref
from pathlib import Path
from shutil import rmtree


def make_ctx(token=None):
    assert config.REPO_PATH, 'no repo selected'
    assert config.HEAD, 'no branch selected'
    return api.Ctx(token, str(config.REPO_PATH), Ref(f'head/{config.HEAD}'))


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
    ctx = make_ctx()
    db = Repo(ctx.path)
    with db.tx():
        return sorted(['/'.join(k.split('/')[1:]) for k in db.heads()])


def create_branch(name):
    ctx = make_ctx()
    db = Repo(ctx.path)
    with db.tx(True):
        db.checkout(ctx.head)
        db.create_branch(Ref(f'head/{name}'), db.head)
    use_branch(name)


def delete_branch(name):
    ctx = make_ctx()
    db = Repo(ctx.path)
    with db.tx(True):
        db.checkout(ctx.head)
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
    ctx = make_ctx()
    db = Repo(ctx.path)
    with db.tx():
        db.checkout(ctx.head)
        return db.ctx(ctx.head).dags.keys()


def delete_dag(name):
    ctx = make_ctx()
    print(ctx)
    db = Repo(ctx.path)
    with db.tx(True):
        db.checkout(ctx.head)
        c = db.ctx(ctx.head)
        if c.dags.pop(name, None):
            c.commit.tree = db(c.tree)
            print([c.head, db(c.commit)])
            db.set_head(ctx.head, db(c.commit))


def invoke_api(token, data):
    return api.invoke(make_ctx(token), data)
