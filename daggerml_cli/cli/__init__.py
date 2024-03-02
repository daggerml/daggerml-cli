import os
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
from daggerml_cli.repo import Error, from_json, to_json

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
@click.option('--config', default='~/config.yml', type=click.Path())  # this allows us to change config path
@click.group(
    no_args_is_help=True,
    context_settings={'help_option_names': ['-h', '--help'], 'auto_envvar_prefix': 'DML', 'show_default': True})
@click.version_option(version=__version__, prog_name='dml')
@clickex
def cli(ctx, config_dir, project_dir, repo, branch, user, repo_path, debug, config):
    if os.path.exists(config):
        with open(config) as f:
            config = load_yaml(f.read())
        ctx.default_map = config
    set_config(ctx)
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
    [click.echo(k) for k in api.list_repo(ctx.obj)]


@repo_group.command(name='gc', help='Delete unreachable objects in the repo.')
@clickex
def repo_gc(ctx):
    click.echo(f'Deleted {api.gc_repo(ctx.obj)} objects.')


@repo_group.command(name='path', help='Filesystem location of the repository.')
@clickex
def repo_path(ctx):
    click.echo(api.repo_path(ctx.obj))


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
    [click.echo(k) for k in api.list_branch(ctx.obj)]


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
@dag_group.command(name='create', help='Create a new DAG.')
@clickex
def api_create_dag(ctx, name, message):
    try:
        cmd = ['begin', [name, message], {}]
        click.echo(to_json(api.invoke_api(ctx.obj, None, cmd)))
    except Exception as e:
        click.echo(to_json(Error.from_ex(e)))


@click.argument('name')
@dag_group.command(name='get', help='Get a DAG.')
@clickex
def api_get_dag(ctx, name):
    click.echo(to_json(api.get_dag(ctx.obj, name)))


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


@click.argument('name', shell_complete=complete(api.list_dag))
@dag_group.command(name='delete', help='Delete a DAG.')
@clickex
def dag_delete(ctx, name):
    api.delete_dag(ctx.obj, name)
    click.echo(f'Deleted DAG: {name}')


@dag_group.command(name='list', help='List DAGs.')
@clickex
def dag_list(ctx):
    [click.echo(k) for k in api.list_dag(ctx.obj)]


###############################################################################
# COMMIT ######################################################################
###############################################################################


@cli.group(name='commit', no_args_is_help=True, help='Commit management commands.')
@clickex
def commit_group(_):
    pass


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
