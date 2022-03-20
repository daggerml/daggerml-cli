import typer
import pkg_resources
import daggerml_cli.users as users
import daggerml_cli.dags as dags
from daggerml_cli.util.config import config
from typing import Optional

app = typer.Typer()

app.add_typer(
    users.app,
    name="users",
    help="Commands related to users and authentication."
)

app.add_typer(
    dags.app,
    name="dags",
    help="Commands for creating, updating, and inspecting DAGs."
)

def print_version(x):
    if x:
        typer.echo(pkg_resources.get_distribution("daggerml-cli").version)
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
        'us-west-2',
        help="The AWS region in which to execute."
    ),
    zone: str = typer.Option(
        'prod',
        help="The zone in which to execute."
    )
):
    """
    DaggerML command line tool.
    """
    config.update({'region': region, 'zone': zone})

if __name__ == "__main__":
    app()
