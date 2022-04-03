import json
import typer
import requests
import daggerml_cli.util.config as config

def print_result(x):
    typer.echo(json.dumps(x, indent=2))
    if x['status'] != 'ok':
        raise typer.Exit(code=1)

def api(data):
    endpoint = f"https://api.{config.zone}-{config.region}.daggerml.com/"
    return requests.post(endpoint, json=data).json()

def print_api(data):
    try:
        print_result(api(data))
    except typer.Exit:
        pass
    except Exception as e:
        xs = str(e).split('\n')
        print_result({
            'status': 'error',
            'error': {
                'code': type(e).__name__,
                'message': xs[0],
                'context': xs[1:],
            }
        })
