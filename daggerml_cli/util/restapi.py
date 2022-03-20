import requests
import os
from daggerml_cli.util.config import config

def api(data):
    host = f"restapi.{config['zone']}-{config['region']}.daggerml.com/"
    endpoint = f"https://{host}"
    resp = requests.post(endpoint, json=data)
    return resp.json()

def get_view(view_name: str):
    return api({"op": "get_view", "args": [view_name]})
