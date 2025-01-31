import subprocess
import sys
from urllib.parse import urlparse


def main():
    proc = subprocess.run(
        [urlparse(sys.argv[1]).path],
        stdin=sys.stdin.read().strip(),
        stdout=subprocess.PIPE,  # stderr passes through to the parent process
        text=True,
    )
    resp = proc.stdout.decode()
    if proc.returncode != 0:
        print(resp, file=sys.stderr)
        sys.exit(1)
    print(resp)
