#!/usr/bin/env python3
import typer
from typing import Optional
from daggerml import users, dags, util, __version__


app = typer.Typer()

app.add_typer(
    users._app,
    name="users",
    help="Commands related to users and authentication."
)

app.add_typer(
    dags._app,
    name="dags",
    help="Commands for creating, updating, and inspecting DAGs."
)


def print_version(x):
    if x:
        typer.echo(f'daggerml-sdk version: {__version__}')
        raise typer.Exit(0)


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=print_version,
        is_eager=True,
        help="Print daggerml version and exit."
    ),
    region: str = typer.Option(
        util.REGION,
        help="The AWS region in which to execute."
    ),
    zone: str = typer.Option(
        util.ZONE,
        help="The zone in which to execute."
    )
):
    """
    DaggerML command line tool.
    """
    util.REGION = region
    util.ZONE = zone
    return
