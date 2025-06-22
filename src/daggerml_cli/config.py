import os
from dataclasses import dataclass, field, fields, replace
from functools import wraps
from typing import Optional

from daggerml_cli.repo import Ref
from daggerml_cli.util import readfile, writefile


class ConfigError(RuntimeError):
    pass


def priv_name(x):
    return f"_{x.upper()}"


def config_property(f=None, **opts):
    def inner(f):
        @wraps(f)
        def getter(self) -> str:
            if base and getattr(self, priv) is None:
                val = os.getenv(env) or readfile(self.get(base), *path)
                setattr(self, priv, val)
            result = f(self) or getattr(self, priv, None)
            if not result:
                errmsg = f"required: --{kebab} option or {env} environment variable"
                errmsg = "%s or `dml %s`" % (errmsg, opts["cmd"]) if opts.get("cmd") else errmsg
                raise ConfigError(errmsg)
            return result

        name = f.__name__
        priv = priv_name(name)
        env = f"DML{priv}"
        kebab = name.lower().replace("_", "-")
        base, *path = opts.get("path", [None])
        result = property(getter)
        if base:

            @result.setter
            def setter(self, value):
                if len(self._writes):
                    self._writes[-1][(self.get(base), *path)] = value
                setattr(self, priv, value)

            return setter
        return result

    return inner if f is None else inner(f)


@dataclass
class Config:
    """This class holds the global configuration options."""

    _CONFIG_DIR: Optional[str] = None
    _PROJECT_DIR: Optional[str] = None
    _REPO: Optional[str] = None
    _BRANCH: Optional[str] = None
    _USER: Optional[str] = None
    _DEBUG: bool = False
    _QUERY: Optional[str] = None
    _writes: list = field(default_factory=list)
    _CACHE_PATH: Optional[str] = None

    @classmethod
    def new(cls, **kw):
        xs = {priv_name(k): v for k, v in kw.items()}
        fs = [f.name for f in fields(cls)]
        return cls(**{k: v for k, v in xs.items() if k in fs})

    def get(self, name, default=None):
        try:
            return getattr(self, name)
        except ConfigError:
            return default

    @property
    def DEBUG(self):
        return self._DEBUG

    @property
    def QUERY(self):
        return self._QUERY

    @config_property
    def CONFIG_DIR(self):
        pass

    @config_property
    def PROJECT_DIR(self):
        pass

    @config_property(path=["PROJECT_DIR", "repo"], cmd="config repo")
    def REPO(self):
        pass

    @config_property(path=["PROJECT_DIR", "head"], cmd="config branch")
    def BRANCH(self):
        pass

    @config_property(path=["CONFIG_DIR", "config", "user"], cmd="config user")
    def USER(self):
        pass

    @property
    def CACHE_PATH(self):
        return self._CACHE_PATH

    @config_property
    def BRANCHREF(self):
        return Ref(f"head/{self.BRANCH}")

    @config_property
    def REPO_DIR(self):
        return os.path.join(self.CONFIG_DIR, "repo")

    @config_property
    def REPO_PATH(self):
        return os.path.join(self.REPO_DIR, self.REPO)

    def replace(self, **changes):
        return replace(self, **changes)

    def __enter__(self):
        self._writes.append({})
        return self

    def __exit__(self, type, *_):
        writes = self._writes.pop()
        if type is None:
            if len(self._writes):
                return self._writes[-1].update(writes)
            [writefile(v, *k) for k, v in writes.items()]
