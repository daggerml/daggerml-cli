import json
import typer
from typing import List, Optional
from daggerml_cli.util import restapi, template

app = typer.Typer()

def print_json(x):
    typer.echo(json.dumps(x, indent=2))

@app.command()
def create_dag(
    template_file: str = typer.Option(
        ...,
        help="The DAG template file, JSON or YAML format."
    ),
    dag_name: str = typer.Option(
        ...,
        help="The name of the created DAG. Must be unique."
    )
):
    """
    Creates a DAG.
    """
    print_json(template.parse(template_file))

@app.command()
def get_nodes():
    """
    Prints nodes to stdout.
    """
    print_json(restapi.get_view("node_view"))

@app.command()
def get_funcs():
    """
    Prints funcs to stdout.
    """
    print_json(restapi.get_view("func_view"))

@app.command()
def get_executors():
    """
    Prints executors to stdout.
    """
    print_json(restapi.get_view("executor_view"))

@app.command()
def get_dags():
    """
    Prints dags to stdout.
    """
    print_json(restapi.get_view("dag_view"))

@app.command()
def get_queue():
    """
    Prints the node work queue to stdout.
    """
    print_json(restapi.get_view("node_queue"))

if __name__ == "__main__":
    app()
