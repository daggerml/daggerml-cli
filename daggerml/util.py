import os
import json
import typer
import requests


ZONE = os.getenv('DML_ZONE', 'prod')
REGION = os.getenv('DML_REGION', 'us-west-2')

def api(data):
    endpoint = f"https://api.{ZONE}-{REGION}.daggerml.com/"
    return requests.post(endpoint, json=data).json()

def print_result(x):
    typer.echo(json.dumps(x, indent=2))
    if x['status'] != 'ok':
        typer.echo('API returned with error status', err=True)
        raise typer.Exit(code=1)
