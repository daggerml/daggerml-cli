import typer
from typing import Optional
from daggerml_cli.util.restapi import print_api

app = typer.Typer()

@app.command()
def create_dag(
    template_file: str = typer.Option(
        ...,
        help="The DAG template file, JSON or YAML format."
    ),
    args: Optional[str] = typer.Option(
        '{}',
        help="A map of arguments to pass to the DAG, EDN format."
    ),
):
    """
    Creates a DAG.
    """
    with open(template_file, 'r') as f:
        print_api({'op': 'eval_dag', 'body': f.read(), 'args': '{}'})

@app.command()
def list_dags():
    """
    Prints a list of DAG names to stdout.
    """
    print_api({'op': 'list_dags'})

@app.command()
def describe_dag(
    dag_id: str = typer.Option(
        ...,
        help="The DAG ID (namespace)."
    ),
):
    """
    Prints DAG info to stdout.
    """
    print_api({'op': 'describe_dag', 'dag_id': dag_id})

if __name__ == "__main__":
    app()
