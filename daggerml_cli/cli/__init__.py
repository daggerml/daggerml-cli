import click
import daggerml_cli.core as core
import json
from click import ClickException
from daggerml_cli.__about__ import __version__
from functools import wraps


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


@cli.group(
    invoke_without_command=True,
    help='Repository management commands.')
@click.pass_context
@clickex
def repo(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(core.current_repo())


@click.argument('name', shell_complete=complete(core.list_repo))
@repo.command(name='create', help='Create a new repository.')
@clickex
def repo_create(name):
    core.create_repo(name)
    click.echo(f'Created repository: {name}')


@click.argument('name', shell_complete=complete(core.list_repo))
@repo.command(name='delete', help='Delete a repository.')
@clickex
def repo_delete(name):
    core.delete_repo(name)
    click.echo(f'Deleted repository: {name}')


@repo.command(name='list', help='List repositories.')
@clickex
def repo_list():
    [click.echo(k) for k in core.list_repo()]


@click.argument('name', shell_complete=complete(core.list_repo))
@repo.command(
    name='use',
    help='Select the repository to use.')
@clickex
def repo_use(name):
    core.use_repo(name)
    click.echo(f'Using repository: {name}')


###############################################################################


@cli.group(
    invoke_without_command=True,
    help='Branch management commands.')
@click.pass_context
@clickex
def branch(ctx):
    if ctx.invoked_subcommand is None:
        click.echo(core.current_branch())


@click.argument('name', shell_complete=complete(core.list_repo))
@branch.command(name='create', help='Create a new branch.')
@clickex
def branch_create(name):
    core.create_branch(name)
    click.echo(f'Created branch: {name}')


@click.argument('name', shell_complete=complete(core.list_repo))
@branch.command(name='delete', help='Delete a branch.')
@clickex
def branch_delete(name):
    core.delete_branch(name)
    click.echo(f'Deleted branch: {name}')


@branch.command(name='list', help='List branches.')
@clickex
def branch_list():
    [click.echo(k) for k in core.list_branch()]


@click.argument('name', shell_complete=complete(core.list_repo))
@branch.command(name='use', help='Select the branch to use.')
@clickex
def branch_use(name):
    core.use_branch(name)
    click.echo(f'Using branch: {name}')


###############################################################################


@cli.group(no_args_is_help=True, help='DAG management commands.')
@clickex
def dag():
    pass


@click.argument('name', shell_complete=complete(core.list_dag))
@dag.command(name='delete', help='Delete a DAG.')
@clickex
def dag_delete(name):
    core.delete_dag(name)
    click.echo(f'Deleted DAG: {name}')


@dag.command(name='list', help='List DAGs.')
@clickex
def dag_list():
    [click.echo(k) for k in core.list_dag()]


###############################################################################


@cli.group(no_args_is_help=True, help='API for creating DAGs.')
@clickex
def api():
    pass


@click.argument('name')
@api.command(name='create-dag', help='Create a new DAG.')
@clickex
def api_create_dag(name):
    click.echo(json.dumps(None, core.invoke_api(['begin', name])))


@click.argument('payload')
@click.argument('token')
@api.command(name='invoke', help='Invoke API with token returned by create-dag.')
@clickex
def api_invoke(token, payload):
    click.echo(json.dumps(core.invoke_api(token, json.loads(payload))))


# @click.option('--token', help='The session token returned from the previous API call.')
# @click.argument('data')
# @api.command(name='create', help='Create a new DAG.')
# @clickex
# def api_create(data):
#     pass
