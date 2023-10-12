import click
import daggerml_cli.api as api
import daggerml_cli.config as config
import os
from click import ClickException
from daggerml_cli.__about__ import __version__
from functools import wraps
from json import loads, dumps


DEBUG = False


def clickex(f):
    @wraps(f)
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except BaseException as e:
            raise e if DEBUG else ClickException(e)
    return inner


def complete(f):
    def inner(ctx, param, incomplete):
        try:
            return [k for k in f() if k.startswith(incomplete)]
        except BaseException:
            return []
    return inner


@click.option('--debug', is_flag=True, help='Enable debug output.')
@click.option('--repo', help='Specify a repo other than the project repo.')
@click.option('--branch', help='Specify a branch other than the current project branch.')
@click.group(context_settings={'help_option_names': ['-h', '--help']}, no_args_is_help=True)
@click.version_option(version=__version__, prog_name='dml')
@clickex
def cli(debug, repo, branch):
    global DEBUG
    DEBUG = debug
    if repo:
        config.REPO = repo
        config.REPO_PATH = os.path.join(config.REPO_DIR, config.REPO)
    if branch:
        config.HEAD = branch


###############################################################################
# REPO ########################################################################
###############################################################################


@cli.group(name='repo', invoke_without_command=True, help='Repository management commands.')
@click.pass_context
@clickex
def repo_group(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(api.current_repo())


@click.argument('name', shell_complete=complete(api.list_repo))
@repo_group.command(name='create', help='Create a new repository.')
@clickex
def repo_create(name):
    api.create_repo(name)
    click.echo(f'Created repository: {name}')


@click.argument('name', shell_complete=complete(api.list_repo))
@repo_group.command(name='delete', help='Delete a repository.')
@clickex
def repo_delete(name):
    api.delete_repo(name)
    click.echo(f'Deleted repository: {name}')


@click.argument('name')
@repo_group.command(name='copy', help='Copy this repository to NAME.')
@clickex
def repo_copy(name):
    api.copy_repo(name)
    click.echo(f'Copied repo: {config.REPO} -> {name}')


@repo_group.command(name='list', help='List repositories.')
@clickex
def repo_list():
    [click.echo(k) for k in api.list_repo()]


@repo_group.command(name='gc', help='Delete unreachable objects in the repo.')
@clickex
def repo_gc():
    click.echo(f'Deleted {api.gc_repo()} objects.')


@repo_group.command(name='path', help='Filesystem location of the repository.')
@clickex
def repo_path():
    click.echo(api.repo_path())


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
    api.init_project(repo)
    click.echo(f'Initialized project with repo: {repo}')


###############################################################################
# BRANCH ######################################################################
###############################################################################


@cli.group(name='branch', invoke_without_command=True, help='Branch management commands.')
@click.pass_context
@clickex
def branch_group(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(api.current_branch())


@click.argument('name')
@branch_group.command(name='create', help='Create a new branch.')
@clickex
def branch_create(name):
    api.create_branch(name)
    click.echo(f'Created branch: {name}')


@click.argument('name', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='delete', help='Delete a branch.')
@clickex
def branch_delete(name):
    api.delete_branch(name)
    click.echo(f'Deleted branch: {name}')


@branch_group.command(name='list', help='List branches.')
@clickex
def branch_list():
    [click.echo(k) for k in api.list_branch()]


@click.argument('name', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='use', help='Select the branch to use.')
@clickex
def branch_use(name):
    api.use_branch(name)
    click.echo(f'Using branch: {name}')


@click.argument('branch', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='merge', help='Merge another branch with the current one.')
@clickex
def branch_merge(branch):
    click.echo(api.merge_branch(branch))


@click.argument('branch', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='rebase', help='Rebase the current branch onto BRANCH.')
@clickex
def branch_rebase(branch):
    click.echo(api.rebase_branch(branch))


###############################################################################
# DAG #########################################################################
###############################################################################


@cli.group(name='dag', no_args_is_help=True, help='DAG management commands.')
@clickex
def dag_group():
    pass


@click.argument('name')
@dag_group.command(name='create', help='Create a new DAG.')
@clickex
def api_create_dag(name):
    click.echo(dumps(api.invoke_api(None, ['begin', name])))


@click.argument('json')
@click.argument('token')
@dag_group.command(name='invoke', help='Invoke API with token returned by create and JSON command.')
@clickex
def api_invoke(token, json):
    click.echo(dumps(api.invoke_api(token, loads(json))))


@click.argument('name', shell_complete=complete(api.list_dag))
@dag_group.command(name='delete', help='Delete a DAG.')
@clickex
def dag_delete(name):
    api.delete_dag(name)
    click.echo(f'Deleted DAG: {name}')


@dag_group.command(name='list', help='List DAGs.')
@clickex
def dag_list():
    [click.echo(k) for k in api.list_dag()]


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
    return api.commit_log_graph()


@click.argument('commit', shell_complete=complete(api.list_commit))
@commit_group.command(name='revert', help='Revert a commit.')
@clickex
def commit_revert(commit):
    return api.revert_commit(commit)
