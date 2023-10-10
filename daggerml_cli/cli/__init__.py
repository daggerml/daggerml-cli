import click
import daggerml_cli.api as api
from click import ClickException
from daggerml_cli.__about__ import __version__
from functools import wraps
from json import loads, dumps


def clickex(f):
    @wraps(f)
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except BaseException as e:
            raise ClickException(e)
    return inner


def complete(f):
    def inner(ctx, param, incomplete):
        try:
            return [k for k in f() if k.startswith(incomplete)]
        except BaseException:
            return []
    return inner


@click.group(
    context_settings={'help_option_names': ['-h', '--help']},
    no_args_is_help=True)
@click.version_option(version=__version__, prog_name='dml')
@clickex
def cli():
    pass


###############################################################################
# REPO ########################################################################
###############################################################################


@cli.group(
    name='repo',
    invoke_without_command=True,
    help='Repository management commands.')
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


@repo_group.command(name='list', help='List repositories.')
@clickex
def repo_list():
    [click.echo(k) for k in api.list_repo()]


@click.argument('name', shell_complete=complete(api.list_repo))
@repo_group.command(
    name='use',
    help='Select the repository to use.')
@clickex
def repo_use(name):
    api.use_repo(name)
    click.echo(f'Using repository: {name}')


###############################################################################
# BRANCH ######################################################################
###############################################################################


@cli.group(
    name='branch',
    invoke_without_command=True,
    help='Branch management commands.')
@click.pass_context
@clickex
def branch_group(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(api.current_branch())


@click.argument('name', shell_complete=complete(api.list_repo))
@branch_group.command(name='create', help='Create a new branch.')
@clickex
def branch_create(name):
    api.create_branch(name)
    click.echo(f'Created branch: {name}')


@click.argument('name', shell_complete=complete(api.list_repo))
@branch_group.command(name='delete', help='Delete a branch.')
@clickex
def branch_delete(name):
    api.delete_branch(name)
    click.echo(f'Deleted branch: {name}')


@branch_group.command(name='list', help='List branches.')
@clickex
def branch_list():
    [click.echo(k) for k in api.list_branch()]


@click.argument('name', shell_complete=complete(api.list_repo))
@branch_group.command(name='use', help='Select the branch to use.')
@clickex
def branch_use(name):
    api.use_branch(name)
    click.echo(f'Using branch: {name}')


###############################################################################
# DAG #########################################################################
###############################################################################


@cli.group(
    name='dag',
    no_args_is_help=True,
    help='DAG management commands.')
@clickex
def dag_group():
    pass


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
# API #########################################################################
###############################################################################


@cli.group(name='api', no_args_is_help=True, help='API for creating DAGs.')
@clickex
def api_group():
    pass


@click.argument('name')
@api_group.command(name='create', help='Create a new DAG.')
@clickex
def api_create_dag(name):
    click.echo(dumps(api.invoke_api(None, ['begin', name])))


@click.argument('json')
@click.argument('token')
@api_group.command(name='invoke', help='Invoke API with token returned by create and JSON command.')
@clickex
def api_invoke(token, json):
    click.echo(dumps(api.invoke_api(token, loads(json))))
