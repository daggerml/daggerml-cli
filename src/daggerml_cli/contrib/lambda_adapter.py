import json
import sys
from urllib.parse import parse_qs, urlparse

import boto3


def main():
    parsed_arn = urlparse(sys.argv[1])
    arn = parsed_arn.scheme + ":" + parsed_arn.netloc + parsed_arn.path
    payload = {
        "dump": sys.stdin.read().strip(),
        "cache_key": sys.argv[2],
        "kwargs": parse_qs(parsed_arn.query),
    }
    response = boto3.client("lambda").invoke(
        FunctionName=arn,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(payload).encode(),
    )
    payload = json.loads(response["Payload"].read())
    if payload["message"] is not None:
        print(payload["message"], file=sys.stderr)
    if payload["status"] // 100 in [4, 5]:
        sys.exit(payload["status"])
    if payload["dump"] is not None:
        print(payload["dump"])
