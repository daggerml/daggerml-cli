import requests

zone = "micha"
region = "us-west-2"
host = f"restapi.{zone}-{region}.daggerml.com/asdf"
endpoint = f"https://{host}"

def api(data):
    resp = requests.post(endpoint, json=data)
    return resp.json()

def get_view(view_name: str):
    return api({"op": "get_view", "args": [view_name]})
