import json
import sys

import boto3


def main():
    # TODO: implement query string to be passed to the lambda.
    response = boto3.client("lambda").invoke(
        FunctionName=sys.argv[1],
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=sys.stdin.read().strip().encode(),
    )
    payload = json.loads(response['Payload'].read())
    if payload["dump"] is not None:
        print(payload["dump"])
