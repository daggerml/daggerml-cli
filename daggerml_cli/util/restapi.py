import requests
import os
import daggerml_cli.util.config as config

def api(data):
    endpoint = f"https://api.{config.zone}-{config.region}.daggerml.com/"
    resp = requests.post(endpoint, json=data)
    return resp.json()

def get_view(view_name: str):
    return api({"op": "get_view", "args": [view_name]})
