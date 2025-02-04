import json
import subprocess
import sys
from shutil import which
from urllib.parse import parse_qs, urlparse


def main():
    prsd = urlparse(sys.argv[1])
    prog = which(prsd.path)
    data = {
        "dump": sys.stdin.read().strip(),
        "cache_key": sys.argv[2],
        "kwargs": parse_qs(prsd.query),
    }
    proc = subprocess.run(
        [prog],
        input=json.dumps(data),
        stdout=subprocess.PIPE,  # stderr passes through to the parent process
        text=True,
    )
    resp = proc.stdout.strip()
    if proc.returncode != 0:
        print(resp, file=sys.stderr)
        sys.exit(1)
    if resp:
        print(resp)
