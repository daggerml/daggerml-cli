import json
import os
from dataclasses import fields
from datetime import datetime
from functools import wraps
from getpass import getuser
from pathlib import Path
from socket import gethostname

import click
from click import ClickException
from yaml import safe_load as load_yaml

from daggerml_cli import api
from daggerml_cli.__about__ import __version__
from daggerml_cli.config import Config
from daggerml_cli.repo import Error, Ref, from_json, to_json

_config_dir = str((Path.home() / '.local/dml').absolute())

DEFAULT_CONFIG = Config(
    os.getenv('DML_CONFIG_DIR', os.path.join(
        str(Path.home()), '.local', 'dml')),
    os.getenv('DML_PROJECT_DIR', '.dml'),
    os.getenv('DML_REPO'),
    os.getenv('DML_BRANCH'),
    os.getenv('DML_USER', f'{getuser()}@{gethostname()}'),
    os.getenv('DML_REPO_PATH'),
)


def jsdump(x, **kw):
    def default(y):
        if isinstance(y, set):
            return list(y)
        if isinstance(y, datetime):
            return y.isoformat()
        if isinstance(y, Ref):
            return y.to
        return str(y)
    return json.dumps(x, default=default, separators=(',', ':'), **kw)


def asdict(x):
    return {f.name: getattr(x, f.name) for f in fields(x)}


def set_config(ctx, *_):
    xs = {f'_{k.upper()}': v for k, v in ctx.params.items()}
    ctx.obj = Config(**{k: v for k, v in xs.items()
                     if hasattr(DEFAULT_CONFIG, k)})


def clickex(f):
    @wraps(f)
    def inner(ctx, *args, **kwargs):
        try:
            return f(ctx, *args, **kwargs)
        except BaseException as e:
            raise (e if ctx.obj.DEBUG else ClickException(str(e))) from e
    return click.pass_context(inner)


def complete(f, prelude=None):
    def inner(ctx, param, incomplete):
        try:
            if prelude:
                prelude(ctx, param, incomplete)
            return [k for k in (f(ctx.obj or DEFAULT_CONFIG) or []) if k.startswith(incomplete)]
        except BaseException:
            return []
    return inner


@click.option(
    '--user',
    default=f'{getuser()}@{gethostname()}',
    help='Specify user name@host or email, etc.')
@click.option(
    '--branch',
    shell_complete=complete(api.list_branch, set_config),
    help='Specify a branch other than the project branch.')
@click.option(
    '--repo-path',
    type=click.Path(),
    help='Specify the path to a repo other than the project repo.')
@click.option(
    '--repo',
    shell_complete=complete(api.list_repo, set_config),
    help='Specify a repo other than the project repo.')
@click.option(
    '--project-dir',
    type=click.Path(),
    default='.dml',
    help='Project directory location.')
@click.option(
    '--config-dir',
    type=click.Path(),
    default=_config_dir,
    help='Config directory location.')
@click.option(
    '--debug',
    is_flag=True,
    help='Enable debug output.')
@click.option('--output', type=click.Choice(['text', 'json']), default='text', help='preferred output format.')
@click.option('--config', default='~/config.yml', type=click.Path())  # this allows us to change config path
@click.group(
    no_args_is_help=True,
    context_settings={'help_option_names': ['-h', '--help'], 'auto_envvar_prefix': 'DML', 'show_default': True})
@click.version_option(version=__version__, prog_name='dml')
@clickex
def cli(ctx, config_dir, project_dir, repo, branch, user, repo_path, debug, config, output):
    if os.path.exists(config):
        with open(config) as f:
            config = load_yaml(f.read())
        ctx.default_map = config
    set_config(ctx)
    ctx.obj.output = output
    ctx.with_resource(ctx.obj)


###############################################################################
# REPO ########################################################################
###############################################################################


@cli.group(name='repo', invoke_without_command=True, help='Repository management commands.')
@clickex
def repo_group(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(api.current_repo(ctx.obj))


@click.argument('name', shell_complete=complete(api.list_repo))
@repo_group.command(name='create', help='Create a new repository.')
@clickex
def repo_create(ctx, name):
    api.create_repo(ctx.obj, name)
    click.echo(f'Created repository: {name}')


@click.argument('name', shell_complete=complete(api.list_repo))
@repo_group.command(name='delete', help='Delete a repository.')
@clickex
def repo_delete(ctx, name):
    api.delete_repo(ctx.obj, name)
    click.echo(f'Deleted repository: {name}')


@click.argument('name')
@repo_group.command(name='copy', help='Copy this repository to NAME.')
@clickex
def repo_copy(ctx, name):
    api.copy_repo(ctx.obj, name)
    click.echo(f'Copied repo: {ctx.obj.REPO} -> {name}')


@repo_group.command(name='list', help='List repositories.')
@clickex
def repo_list(ctx):
    output = ctx.obj.output
    for k in api.list_repo(ctx.obj):
        if output == 'json':
            click.echo(json.dumps({'name': k}))
        else:
            click.echo(k)


@repo_group.command(name='gc', help='Delete unreachable objects in the repo.')
@clickex
def repo_gc(ctx):
    for rsrc in api.gc_repo(ctx.obj):
        click.echo(rsrc.uri)


@repo_group.command(name='path', help='Filesystem location of the repository.')
@clickex
def repo_path(ctx):
    click.echo(api.repo_path(ctx.obj))


@click.argument('ref', type=str)
@repo_group.command(name='dump-ref', help='dump a ref and all its dependencies to json')
@clickex
def dag_dump_ref(ctx, ref):
    dump = api.dump_ref(ctx.obj, from_json(ref))
    click.echo(dump)


@repo_group.command(name='load-ref', help='load a ref and all its dependencies into the db')
@click.argument('js', type=str)
@clickex
def load_dump_ref(ctx, js):
    ref = api.load_ref(ctx.obj, js)
    click.echo(to_json(ref))


###############################################################################
# PROJECT #####################################################################
###############################################################################


@cli.group(name='project', no_args_is_help=True, help='Project management commands.')
@clickex
def project_group(_):
    pass


@click.argument('repo', shell_complete=complete(api.list_repo))
@project_group.command(name='init', help='Associate a project with a REPO.')
@clickex
def project_init(ctx, repo):
    api.init_project(ctx.obj, repo)
    click.echo(f'Initialized project with repo: {repo}')


###############################################################################
# BRANCH ######################################################################
###############################################################################


@cli.group(name='branch', invoke_without_command=True, help='Branch management commands.')
@clickex
def branch_group(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(api.current_branch(ctx.obj))


@click.argument('name')
@branch_group.command(name='create', help='Create a new branch.')
@clickex
def branch_create(ctx, name):
    api.create_branch(ctx.obj, name)
    click.echo(f'Created branch: {name}')


@click.argument('name', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='delete', help='Delete a branch.')
@clickex
def branch_delete(ctx, name):
    api.delete_branch(ctx.obj, name)
    click.echo(f'Deleted branch: {name}')


@branch_group.command(name='list', help='List branches.')
@clickex
def branch_list(ctx):
    output = ctx.obj.output
    for k in api.list_branch(ctx.obj):
        if output == 'json':
            click.echo(json.dumps({'name': k}))
        else:
            click.echo(k)


@click.argument('name', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='use', help='Select the branch to use.')
@clickex
def branch_use(ctx, name):
    api.use_branch(ctx.obj, name)
    click.echo(f'Using branch: {name}')


@click.argument('branch', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='merge', help='Merge another branch with the current one.')
@clickex
def branch_merge(ctx, branch):
    click.echo(api.merge_branch(ctx.obj, branch))


@click.argument('branch', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='rebase', help='Rebase the current branch onto BRANCH.')
@clickex
def branch_rebase(ctx, branch):
    click.echo(api.rebase_branch(ctx.obj, branch))


###############################################################################
# DAG #########################################################################
###############################################################################


@cli.group(name='dag', no_args_is_help=True, help='DAG management commands.')
@clickex
def dag_group(_):
    pass


@click.argument('message')
@click.argument('name')
@click.option('-d', '--dag-dump', help='dag dump', type=str)
@dag_group.command(name='create', help='Create a new DAG.')
@clickex
def dag_create(ctx, name, message, dag_dump=None):
    try:
        idx = api.begin_dag(ctx.obj, name=name, message=message, dag_dump=dag_dump)
        click.echo(to_json(idx))
    except Exception as e:
        click.echo(to_json(Error.from_ex(e)))


@dag_group.command(name='describe', help='Get a DAG.')
@click.argument('id')
@clickex
def describe_dag(ctx, id):
    click.echo(jsdump(api.describe_dag(ctx.obj, id)))


@dag_group.command(name='list', help='List DAGs.')
@click.option('-n', '--dag-names', multiple=True)
@clickex
def dag_list(ctx, dag_names):
    output = ctx.obj.output
    for k in api.list_dags(ctx.obj, dag_names=dag_names):
        if output == 'json':
            dag = k.pop('dag')
            out = {**k, **asdict(dag)}
            if isinstance(out['result'], dict):
                out['result'] = out['result']['to']
            out.pop('nodes')
            click.echo(jsdump(out))
        else:
            click.echo(k['id'])


@click.argument('json')
@click.argument('token')
@dag_group.command(
    name='invoke',
    help=f'Invoke API with token returned by create and JSON command.\n\nops: {list(api._invoke_method.fn_map.keys())}')
@clickex
def api_invoke(ctx, token, json):
    try:
        click.echo(to_json(api.invoke_api(
            ctx.obj, from_json(token), from_json(json))))
    except Exception as e:
        click.echo(to_json(Error.from_ex(e)))


###############################################################################
# INDEX #######################################################################
###############################################################################


@cli.group(name='index', no_args_is_help=True, help='Index management commands.')
@clickex
def index_group(_):
    pass


@index_group.command(name='list', help="List indexes.")
@clickex
def index_list(ctx):
    output = ctx.obj.output
    for k in api.list_indexes(ctx.obj):
        if output == 'json':
            dag = k.pop('index')
            click.echo(jsdump({**k, **asdict(dag)}))
        else:
            click.echo(k['id'])


@index_group.command(name='delete', help="delete index.")
@click.argument('id')
@clickex
def index_delete(ctx, id):
    click.echo(api.delete_index(ctx.obj, Ref(id)))


###############################################################################
# COMMIT ######################################################################
###############################################################################


@cli.group(name='commit', no_args_is_help=True, help='Commit management commands.')
@clickex
def commit_group(_):
    pass


@commit_group.command(name='list', help='List commits.')
@clickex
def commit_list(ctx):
    output = ctx.obj.output
    for id, commit in api.list_commit(ctx.obj):
        if output == 'json':
            click.echo(jsdump(dict(id=id, **asdict(commit))))
        else:
            click.echo(id)


@click.option('--graph', is_flag=True, help='Print a graph of all commits.')
@commit_group.command(name='log', help='Query the commit log.')
@clickex
def commit_log(ctx, graph):
    return api.commit_log_graph(ctx.obj)


@click.argument('commit', shell_complete=complete(api.list_commit))
@commit_group.command(name='revert', help='Revert a commit.')
@clickex
def commit_revert(ctx, commit):
    return api.revert_commit(ctx.obj, commit)
