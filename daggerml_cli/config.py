import os
from daggerml_cli.util import readfile, writefile
from daggerml_cli.repo import Ref
from dataclasses import dataclass, replace
from functools import wraps


class ConfigError(RuntimeError):
    pass


def config_property(f=None, **opts):
    def inner(f):
        @wraps(f)
        def getter(self):
            if base and getattr(self, priv) is None:
                setattr(self, priv, readfile(self.get(base), *path))
            result = f(self) or getattr(self, priv, None)
            if not result:
                errmsg = f'required: --{kebab} option or DML_{name} environment variable'
                errmsg = '%s or `dml %s`' % (errmsg, opts['cmd']) if opts.get('cmd') else errmsg
                raise ConfigError(errmsg)
            return result
        name = f.__name__
        priv = f'_{name}'
        kebab = name.lower().replace('_', '-')
        base, *path = opts.get('path', [None])
        result = property(getter)
        if base:
            @result.setter
            def setter(self, value):
                writefile(value, self.get(base), *path)
                setattr(self, priv, value)
            return setter
        return result
    return inner if f is None else inner(f)


@dataclass
class Config:
    _CONFIG_DIR: str = None
    _PROJECT_DIR: str = None
    _REPO: str = None
    _BRANCH: str = None
    _USER: str = None
    _REPO_PATH: str = None
    DEBUG: bool = False

    def get(self, name, default=None):
        try:
            return getattr(self, name)
        except ConfigError:
            return default

    @config_property
    def CONFIG_DIR(self):
        pass

    @config_property
    def PROJECT_DIR(self):
        pass

    @config_property(path=['PROJECT_DIR', 'repo'], cmd='project init')
    def REPO(self):
        pass

    @config_property(path=['PROJECT_DIR', 'head'], cmd='branch use')
    def BRANCH(self):
        pass

    @config_property(path=['CONFIG_DIR', 'config', 'user'], cmd='config set user')
    def USER(self):
        pass

    @config_property
    def BRANCHREF(self):
        return Ref(f'head/{self.BRANCH}')

    @config_property
    def REPO_DIR(self):
        return os.path.join(self.CONFIG_DIR, 'repo')

    @config_property
    def REPO_PATH(self):
        return os.path.join(self.REPO_DIR, self.REPO)

    def replace(self, **changes):
        return replace(self, **changes)
