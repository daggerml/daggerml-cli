import subprocess
import sys
from shutil import which
from urllib.parse import urlparse


def main():
    prog = which(urlparse(sys.argv[1]).path)
    proc = subprocess.run(
        [prog],
        stdin=sys.stdin,
        stdout=subprocess.PIPE,  # stderr passes through to the parent process
        text=True,
    )
    resp = proc.stdout.strip()
    if proc.returncode != 0:
        print(resp, file=sys.stderr)
        sys.exit(1)
    if resp:
        print(resp)
