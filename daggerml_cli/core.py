import daggerml_cli.config as config
import os
from click import ClickException
from daggerml_cli.repo import Repo, Ref
from pathlib import Path
from shutil import rmtree


def current_db():
    return config.DB


def list_dbs():
    return os.listdir(config.DB_DIR)


def create_db(name):
    path = Path.joinpath(config.DB_DIR, name)
    Repo(str(path), create=True)
    use_db(name)


def delete_db(name):
    path = Path.joinpath(config.DB_DIR, name)
    rmtree(str(path))
    if current_db() == name:
        use_db(None)


def use_db(name):
    if name is None:
        os.remove(config.DB_CONFIG_FILE)
        config.DB_PATH = None
    else:
        assert name in list_dbs(), f'database not found: {name}'
        os.makedirs(config.CONFIG_DIR, mode=0o700, exist_ok=True)
        with open(config.DB_CONFIG_FILE, 'w') as f:
            f.write(name+'\n')
        config.DB_PATH = Path.joinpath(config.DB_DIR, name)
    config.DB = name


def current_branch():
    return config.HEAD


def list_branches():
    db = Repo(str(config.DB_PATH))
    with db.tx():
        return ['/'.join(k.split('/')[1:]) for k in db.heads()]


def create_branch(name):
    db = Repo(str(config.DB_PATH))
    with db.tx(True):
        db.create_branch(Ref(f'head/{name}'), db.head)
    use_branch(name)


def delete_branch(name):
    pass


def use_branch(name):
    if name is None:
        pass
    else:
        assert name in list_branches(), f'branch not found: {name}'
        os.makedirs(config.CONFIG_DIR, mode=0o700, exist_ok=True)
        with open(config.HEAD_CONFIG_FILE, 'w') as f:
            f.write(name+'\n')
    config.HEAD = name
