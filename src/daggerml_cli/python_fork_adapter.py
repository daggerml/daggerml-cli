import os
import sys
from urllib.parse import urlparse


def main():
    os.execl(sys.executable, sys.executable, urlparse(sys.argv[1]).path)  # noqa: S606
