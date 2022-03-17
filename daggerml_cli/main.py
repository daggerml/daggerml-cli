import typer
import pkg_resources
import daggerml_cli.users
import daggerml_cli.dags
from typing import Optional

app = typer.Typer()

app.add_typer(
        daggerml_cli.users.app,
        name="users",
        help="Commands related to users and authentication."
        )

app.add_typer(
        daggerml_cli.dags.app,
        name="dags",
        help="Commands for creating, updating, and inspecting DAGs."
        )

def print_version(x):
    if x:
        typer.echo(pkg_resources.get_distribution("daggerml-cli").version)
        raise typer.Exit(0)

@app.callback()
def main(
        ctx: typer.Context,
        version: Optional[bool] = typer.Option(
            None,
            "--version",
            callback=print_version,
            is_eager=True,
            help="Print daggerml version and exit."
            )
        ):
    """
    DaggerML command line tool.
    """

if __name__ == "__main__":
    app()
