import click
import daggerml_cli.core as core
import daggerml_cli.config as config
import daggerml_cli.util as util
import daggerml_cli.repo as repo
import os
from click import ClickException
from daggerml_cli.__about__ import __version__
from functools import wraps
from pathlib import Path


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
@click.version_option(
    version=__version__,
    prog_name='dml')
@clickex
def cli():
    pass


@click.option(
    '--list',
    is_flag=True,
    help='List databases.')
@click.option(
    '--create',
    default=None,
    help='Create a new database.')
@click.option(
    '--delete',
    default=None,
    shell_complete=complete(core.list_dbs),
    help='Delete a database.')
@click.option(
    '--use',
    default=None,
    shell_complete=complete(core.list_dbs),
    help='Select a database to use.')
@cli.command(
    help='Database management.')
@clickex
def db(list, create, delete, use):
    if list:
        [click.echo(k) for k in core.list_dbs()]
    elif create:
        core.create_db(create)
        click.echo(f'Database created: {create}')
    elif delete:
        core.delete_db(delete)
        click.echo(f'Database deleted: {delete}')
    elif use:
        core.use_db(use)
        click.echo(f'Using database: {use}')
    else:
        click.echo(core.current_db())


@click.option(
    '--list',
    is_flag=True,
    help='List branches.')
@click.option(
    '--create',
    default=None,
    help='Create a new branch.')
@click.option(
    '--delete',
    default=None,
    shell_complete=complete(core.list_branches),
    help='Delete a branch.')
@click.option(
    '--use',
    default=None,
    shell_complete=complete(core.list_branches),
    help='Select a branch to use.')
@cli.command(
    help='Branch management.')
@clickex
def branch(list, create, delete, use):
    if list:
        [click.echo(k) for k in core.list_branches()]
    elif create:
        core.create_branch(create)
        click.echo(f'Branch created: {create}')
    elif delete:
        core.delete_branch(delete)
        click.echo(f'Branch deleted: {delete}')
    elif use:
        core.use_branch(use)
        click.echo(f'Using branch: {use}')
    else:
        click.echo(core.current_branch())
