import json
import logging
import sys
from urllib.parse import urlparse

import boto3
import click
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


def get_path(uri):
    uri = urlparse(uri)
    assert uri.scheme == "s3", f"invalid URI scheme: {uri.scheme}"
    return uri.netloc, f"{uri.path[1:]}/data.mdb"


def code(ex: ClientError) -> str:
    return ex.response["Error"]["Code"]


@click.group(
    no_args_is_help=True,
    context_settings=dict(help_option_names=["-h", "--help"]),
)
def main():
    """DaggerML remote S3 protocol handler."""


@click.argument("uri")
@main.command()
def tag(uri):
    """Get the hash of S3 object.
    The URI is the S3 location."""
    bucket, key = get_path(uri)
    etag = ""
    try:
        etag = json.loads(boto3.resource("s3").Bucket(bucket).Object(key).e_tag)
    except ClientError as e:
        if code(e) != "404":
            raise
    sys.stdout.write(etag)


@click.argument("tag")
@click.argument("uri")
@main.command()
def get(uri, tag):
    """Get the contents of S3 object.
    The URI is the S3 location and the TAG is the hash of the object returned by
    the `tag` command. Contents are written to stdout."""
    bucket, key = get_path(uri)
    try:
        resp = boto3.client("s3").get_object(
            Bucket=bucket,
            Key=key,
            **(dict(IfNoneMatch="*") if tag == "" else dict(IfMatch=tag)),
        )
        sys.stdout.buffer.write(resp["Body"].read())
    except ClientError as e:
        if code(e) == "PreconditionFailed":
            raise Exception("contents changed: please try again") from e
        raise


@click.argument("tag")
@click.argument("uri")
@main.command()
def put(uri, tag):
    """Write the contents of S3 object.
    The URI is the S3 location and the TAG is the hash of the object returned by
    the `tag` command. Contents to write are read from stdin."""
    bucket, key = get_path(uri)
    try:
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key,
            Body=sys.stdin.buffer.read(),
            **(dict(IfNoneMatch="*") if tag == "" else dict(IfMatch=tag)),
        )
    except ClientError as e:
        if code(e) == "PreconditionFailed":
            raise Exception("contents changed: please try again") from e
        raise
