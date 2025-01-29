import json
import sys

import boto3


def main():
    # TODO: implement query string to be passed to the lambda.
    response = boto3.client("lambda").invoke(
        FunctionName=sys.argv[1],
        InvocationType='RequestResponse',
        LogType='Tail',
        # extra json.dumps because lambda will json.loads
        Payload=json.dumps(sys.stdin.read().strip()).encode(),
    )
    payload = json.loads(response['Payload'].read())
    if payload["context"] is not None:
        print(payload["context"], file=sys.stderr)
    if payload["dump"] is not None:
        print(payload["dump"])
