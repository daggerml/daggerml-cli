import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory

from tabulate import tabulate

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import Ref, Repo


def dump(repo, count=None):
    rows = []
    for db in repo.dbs.keys():
        [rows.append([len(rows) + 1, k.to, k()]) for k in repo.cursor(db)]
    rows = rows[:min(count, len(rows))] if count is not None else rows
    print('\n' + tabulate(rows, tablefmt="simple_grid"))


@dataclass
class SimpleApi:
    token: Ref
    ctx: Config
    tmpdirs: list = field(default_factory=list)

    def __getattr__(self, name):
        def invoke(*args, **kwargs):
            return api.invoke_api(self.ctx, self.token, [name, args, kwargs])
        return invoke

    @classmethod
    def begin(
            cls,
            name='test',
            message='test',
            user='test',
            config_dir=None,
            fn_cache_dir='',
            ctx=None,
            dump=None
    ):
        tmpdirs = []
        if ctx is None:
            tmpdirs = [TemporaryDirectory() for _ in range(2)]
            ctx = Config(
                _CONFIG_DIR=(config_dir or tmpdirs[0].__enter__()),
                _PROJECT_DIR=tmpdirs[1].__enter__(),
                _USER=user,
            )
            os.environ['DML_FN_CACHE_DIR'] = fn_cache_dir
            if 'test' not in [x['name'] for x in api.list_repo(ctx)]:
                api.create_repo(ctx, 'test')
            api.config_repo(ctx, 'test')
        tok = api.begin_dag(ctx, name=name, message=message, dump=dump)
        return cls(tok, ctx, tmpdirs)

    @contextmanager
    def tx(self, write=False):
        db = Repo(self.ctx.REPO_PATH, self.ctx.USER, self.ctx.BRANCHREF)
        with db.tx(write):
            yield db

    def start_fn(self, *args, **kwargs):
        kwargs['argv'] = kwargs.get('argv', [self.put_literal(x) for x in args])
        return api.invoke_api(self.ctx, self.token, ['start_fn', [], kwargs])

    def dump_ref(self, ref):
        return api.dump_ref(self.ctx, ref)

    def cleanup(self):
        os.environ.pop('DML_FN_CACHE_DIR', None)
        for x in self.tmpdirs:
            x.__exit__(None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, *x):
        self.cleanup()
