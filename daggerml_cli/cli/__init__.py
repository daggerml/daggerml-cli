import click
import daggerml_cli.api as api
import daggerml_cli.config as config
import os
from click import ClickException
from daggerml_cli.__about__ import __version__
from functools import wraps
from json import loads, dumps
from pathlib import Path


DEBUG = False
CTX = config.Config(
    False,
    os.getenv('DML_CONFIG_DIR', os.path.join(str(Path.home()), '.local', 'dml')),
    os.getenv('DML_PROJECT_DIR', '.dml'),
    os.getenv('DML_REPO'),
    os.getenv('DML_BRANCH'),
    os.getenv('DML_USER'),
)


def clickex(f):
    @wraps(f)
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except BaseException as e:
            raise e if CTX.DEBUG else ClickException(e)
    return inner


def complete(f):
    def inner(ctx, param, incomplete):
        try:
            return [k for k in f(CTX) if k.startswith(incomplete)]
        except BaseException:
            return []
    return inner


@click.option(
    '--debug',
    is_flag=True,
    help='Enable debug output.')
@click.option(
    '--config-dir',
    default=CTX.CONFIG_DIR,
    help='Config directory location.')
@click.option(
    '--project-dir',
    default=CTX.PROJECT_DIR,
    help='Project directory location.')
@click.option(
    '--repo',
    default=CTX.REPO,
    help='Specify a repo other than the project repo.')
@click.option(
    '--branch',
    default=CTX.HEAD,
    help='Specify a branch other than the project branch.')
@click.option(
    '--user',
    default=CTX.USER,
    help='Specify user email.')
@click.group(
    no_args_is_help=True,
    context_settings={'help_option_names': ['-h', '--help']})
@click.version_option(version=__version__, prog_name='dml')
@clickex
def cli(debug, config_dir, project_dir, repo, branch, user):
    global CTX
    CTX = config.Config(debug, config_dir, project_dir, repo, branch, user)


###############################################################################
# REPO ########################################################################
###############################################################################


@cli.group(name='repo', invoke_without_command=True, help='Repository management commands.')
@click.pass_context
@clickex
def repo_group(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(api.current_repo(CTX))


@click.argument('name', shell_complete=complete(api.list_repo))
@repo_group.command(name='create', help='Create a new repository.')
@clickex
def repo_create(name):
    api.create_repo(CTX, name)
    click.echo(f'Created repository: {name}')


@click.argument('name', shell_complete=complete(api.list_repo))
@repo_group.command(name='delete', help='Delete a repository.')
@clickex
def repo_delete(name):
    api.delete_repo(CTX, name)
    click.echo(f'Deleted repository: {name}')


@click.argument('name')
@repo_group.command(name='copy', help='Copy this repository to NAME.')
@clickex
def repo_copy(name):
    api.copy_repo(CTX, name)
    click.echo(f'Copied repo: {CTX.REPO} -> {name}')


@repo_group.command(name='list', help='List repositories.')
@clickex
def repo_list():
    [click.echo(k) for k in api.list_repo(CTX)]


@repo_group.command(name='gc', help='Delete unreachable objects in the repo.')
@clickex
def repo_gc():
    click.echo(f'Deleted {api.gc_repo(CTX)} objects.')


@repo_group.command(name='path', help='Filesystem location of the repository.')
@clickex
def repo_path():
    click.echo(api.repo_path(CTX))


###############################################################################
# PROJECT #####################################################################
###############################################################################


@cli.group(name='project', no_args_is_help=True, help='Project management commands.')
@clickex
def project_group():
    pass


@click.argument('repo', shell_complete=complete(api.list_repo))
@project_group.command(name='init', help='Associate a project with a REPO.')
@clickex
def project_init(repo):
    api.init_project(CTX, repo)
    click.echo(f'Initialized project with repo: {repo}')


###############################################################################
# BRANCH ######################################################################
###############################################################################


@cli.group(name='branch', invoke_without_command=True, help='Branch management commands.')
@click.pass_context
@clickex
def branch_group(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(api.current_branch(CTX))


@click.argument('name')
@branch_group.command(name='create', help='Create a new branch.')
@clickex
def branch_create(name):
    api.create_branch(CTX, name)
    click.echo(f'Created branch: {name}')


@click.argument('name', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='delete', help='Delete a branch.')
@clickex
def branch_delete(name):
    api.delete_branch(CTX, name)
    click.echo(f'Deleted branch: {name}')


@branch_group.command(name='list', help='List branches.')
@clickex
def branch_list():
    [click.echo(k) for k in api.list_branch(CTX)]


@click.argument('name', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='use', help='Select the branch to use.')
@clickex
def branch_use(name):
    api.use_branch(CTX, name)
    click.echo(f'Using branch: {name}')


@click.argument('branch', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='merge', help='Merge another branch with the current one.')
@clickex
def branch_merge(branch):
    click.echo(api.merge_branch(CTX, branch))


@click.argument('branch', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='rebase', help='Rebase the current branch onto BRANCH.')
@clickex
def branch_rebase(branch):
    click.echo(api.rebase_branch(CTX, branch))


###############################################################################
# DAG #########################################################################
###############################################################################


@cli.group(name='dag', no_args_is_help=True, help='DAG management commands.')
@clickex
def dag_group():
    pass


@click.argument('message')
@click.argument('name')
@dag_group.command(name='create', help='Create a new DAG.')
@clickex
def api_create_dag(name, message):
    click.echo(dumps(api.invoke_api(CTX, None, ['begin', name, CTX.USER, message])))


@click.argument('json')
@click.argument('token')
@dag_group.command(name='invoke', help='Invoke API with token returned by create and JSON command.')
@clickex
def api_invoke(token, json):
    click.echo(dumps(api.invoke_api(CTX, token, loads(json))))


@click.argument('name', shell_complete=complete(api.list_dag))
@dag_group.command(name='delete', help='Delete a DAG.')
@clickex
def dag_delete(name):
    api.delete_dag(CTX, name)
    click.echo(f'Deleted DAG: {name}')


@dag_group.command(name='list', help='List DAGs.')
@clickex
def dag_list():
    [click.echo(k) for k in api.list_dag(CTX)]


###############################################################################
# COMMIT ######################################################################
###############################################################################


@cli.group(name='commit', no_args_is_help=True, help='Commit management commands.')
@clickex
def commit_group():
    pass


@click.option('--graph', is_flag=True, help='Print a graph of all commits.')
@commit_group.command(name='log', help='Query the commit log.')
@clickex
def commit_log(graph):
    return api.commit_log_graph(CTX)


@click.argument('commit', shell_complete=complete(api.list_commit))
@commit_group.command(name='revert', help='Revert a commit.')
@clickex
def commit_revert(commit):
    return api.revert_commit(CTX, commit)
