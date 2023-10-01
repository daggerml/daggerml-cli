# SPDX-FileCopyrightText: 2023-present Micha Niskin <micha.niskin@gmail.com>
#
# SPDX-License-Identifier: MIT
import click

from daggerml_cli.__about__ import __version__


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True)
@click.version_option(
    version=__version__,
    prog_name="dml")
def cli():
    raise click.UsageError('no command specified')


@cli.command(
    help='Create a new DAG.')
def create_dag():
    print('create dag!')
