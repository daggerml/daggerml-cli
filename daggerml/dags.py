import os
import typer
from daggerml.util import api, print_result

_app = typer.Typer(result_callback=print_result)


@_app.command()
def create_dag(template: str, args: str = '{}'):
    """Creates a DAG.

    Parameters
    ----------
    template : str
        if it's a valid filepath, this script will read that file and submit it
        to the API.
        if it's not a valid filepath, this must be valid EDN and will be
        submitted to the API.
    args : str, optional
        edn formatted map of args to pass in to the template
    """
    if os.path.isfile(template):
        with open(template, 'r') as f:
            template = f.read()
    return api({'op': 'eval_dag', 'body': template, 'args': args})


@_app.command()
def list_dags():
    """Lists DAGs by name
    """
    return api({'op': 'list_dags'})


@_app.command()
def describe_dag(dag_id: str):
    """Prints DAG info to stdout.

    Parameters
    ----------
    dag_id : str
        the ID of the dag (e.g. `com.daggerml.builtin:v0.0.1`)
    """
    return api({'op': 'describe_dag', 'dag_id': dag_id})
