import logging
import os
import sys
from hashlib import md5
from urllib.parse import urlparse, urlunparse

import click
from filelock import FileLock

log = logging.getLogger(__name__)
DEFAULT_TAG = "00000000000000000000000000000000"


def get_path(uri):
    uri = urlparse(uri)
    path = os.path.join(uri.path, "data.mdb")
    assert uri.scheme == "file", f"invalid URI scheme: {uri.scheme}"
    assert os.path.isabs(path), f"invalid URI: absolute path required: {urlunparse(uri)}"
    return path


def check_tag(path, tag=None):
    etag = DEFAULT_TAG
    try:
        with open(path, "rb") as f:
            etag = md5(f.read()).hexdigest()
    except FileNotFoundError:
        pass
    if tag:
        assert etag == tag, "contents changed: please try again"
    return etag


@click.group(
    no_args_is_help=True,
    context_settings=dict(help_option_names=["-h", "--help"]),
)
def main():
    """DaggerML remote file protocol handler."""


@click.argument("uri")
@main.command()
def tag(uri):
    """Get the hash of file.
    The URI is the file location."""
    path = get_path(uri)
    with FileLock(f"{path}.lock"):
        sys.stdout.write(check_tag(path))


@click.argument("tag")
@click.argument("uri")
@main.command()
def get(uri, tag):
    """Get the contents of file.
    The URI is the file location and the TAG is the hash of the file returned by
    the `tag` command. Contents are written to stdout."""
    path = get_path(uri)
    with FileLock(f"{path}.lock"):
        check_tag(path, tag)
        with open(path, "rb") as f:
            sys.stdout.buffer.write(f.read())


@click.argument("tag")
@click.argument("uri")
@main.command()
def put(uri, tag):
    """Write the contents of file.
    The URI is the file location and the TAG is the hash of the file returned by
    the `tag` command. Contents to write are read from stdin."""
    path = get_path(uri)
    with FileLock(f"{path}.lock"):
        check_tag(path, tag)
        with open(path, "wb") as f:
            f.write(sys.stdin.buffer.read())
