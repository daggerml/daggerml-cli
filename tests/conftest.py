"""Common test fixtures for dml-util tests."""

import logging
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict
from unittest.mock import patch

import pytest

from daggerml_cli._db import DmlDbEnv
from daggerml_cli.ops.base_ops import BaseOps
from daggerml_cli.types import NAMESPACES


@pytest.fixture(scope="module")
def _aws_server():
    """Module fixture providing a moto S3 server."""
    with patch.dict(os.environ):
        # IMPORTANT: clear out env variables for safety **BEFORE** importing moto
        for k in os.environ:
            if k.startswith("AWS_"):
                del os.environ[k]
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer(port=0, verbose=False)
        server.start()
        host, port = server.get_host_and_port()
        yield {
            "server": server,
            "endpoint": f"http://{host}:{port}",
            "port": port,
            "envvars": {
                "AWS_ACCESS_KEY_ID": "test",
                "AWS_SECRET_ACCESS_KEY": "test",
                "AWS_REGION": "us-east-1",
                "AWS_DEFAULT_REGION": "us-east-1",
                "AWS_ENDPOINT_URL": f"http://{host}:{port}",
            },
        }
        server.stop()


@pytest.fixture(autouse=True)
def clear_envvars():
    """Autouse fixture to clear AWS/DML env vars and set test values."""
    with patch.dict(os.environ):
        # Clear existing AWS and DML environment variables
        for k in list(os.environ.keys()):
            if k.startswith("AWS_") or k.startswith("DML_"):
                del os.environ[k]

        # Set test-specific environment variables
        os.environ["DML_S3_BUCKET"] = "test-bucket"
        os.environ["DML_S3_PREFIX"] = "test-prefix"
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        os.environ["PYTHONPATH"] = "."  # ensure `tests` is in PYTHONPATH
        yield


@pytest.fixture
def aws_server(_aws_server, clear_envvars):
    """Fixture that sets up AWS environment and returns server info."""
    import boto3

    # Set environment variables from _aws_server
    os.environ.update(_aws_server["envvars"])
    # Call boto3.setup_default_session() after env vars are set
    boto3.setup_default_session()
    yield _aws_server


@pytest.fixture
def s3(aws_server):
    """Fixture providing a boto3 S3 client and ensuring bucket exists."""
    import boto3

    s3_client = boto3.client("s3", endpoint_url=aws_server["endpoint"])
    bucket = os.environ["DML_S3_BUCKET"]
    try:
        s3_client.create_bucket(Bucket=bucket)
    except s3_client.exceptions.BucketAlreadyExists:
        pass  # Bucket already exists, which is fine
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass  # Bucket already owned by us, which is fine
    yield s3_client


@pytest.fixture
def db():
    """Fixture providing a FakeDb instance for testing."""
    return FakeDb()


@pytest.fixture
def remote_ops(db, s3):
    """Fixture providing RemoteOps instance using plain boto3.client('s3')."""
    from daggerml_cli.ops.remote import RemoteOps

    yield RemoteOps(db, s3)


@pytest.fixture(scope="class")
def integration_remote_ops(temp_bo, aws_server):
    """Fixture providing RemoteOps instance with real database for integration tests."""
    import boto3

    from daggerml_cli.ops.remote import RemoteOps

    # Create S3 client for integration tests
    s3_client = boto3.client("s3", endpoint_url=aws_server["endpoint"])

    # Ensure bucket exists
    bucket = os.environ["DML_S3_BUCKET"]
    try:
        s3_client.create_bucket(Bucket=bucket)
    except s3_client.exceptions.BucketAlreadyExists:
        pass  # Bucket already exists, which is fine
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass  # Bucket already owned by us, which is fine

    yield RemoteOps(temp_bo, s3_client)


@pytest.fixture
def temp_db_fn():
    """Function-scoped fixture providing a temporary DmlDbEnv for integration tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            db_env = TmpEnv.create(temp_dir, namespaces=sorted(NAMESPACES))
            yield db_env
        finally:
            db_env.clear_all()
            db_env.close()


@pytest.fixture
def temp_bo_fn(temp_db_fn):
    """Function-scoped fixture providing a BaseOps instance with a temporary database."""
    yield BaseOps(temp_db_fn)


@pytest.fixture
def integration_remote_ops_fn(temp_bo_fn, aws_server):
    """Function-scoped fixture providing RemoteOps instance with real database for integration tests."""
    import boto3

    from daggerml_cli.ops.remote import RemoteOps

    # Create S3 client for integration tests
    s3_client = boto3.client("s3", endpoint_url=aws_server["endpoint"])

    # Ensure bucket exists
    bucket = os.environ["DML_S3_BUCKET"]
    try:
        s3_client.create_bucket(Bucket=bucket)
    except s3_client.exceptions.BucketAlreadyExists:
        pass  # Bucket already exists, which is fine
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass  # Bucket already owned by us, which is fine

    yield RemoteOps(temp_bo_fn._db, s3_client)


@dataclass
class FakeTxn:
    """Fake transaction implementation matching required interface."""

    kv: Dict[str, Any]
    readonly: bool

    def get(self, ref):
        """Get value by ref from fake storage."""
        return self.kv.get(ref.to)

    def put(self, value, *, to=None, **kwargs):
        """Put value at ref in fake storage."""
        if self.readonly:
            raise ValueError("Cannot put in readonly transaction")
        self.kv[to.to] = value
        return to


@dataclass
class FakeDb:
    """Fake database implementation matching required interface."""

    kv: Dict[str, Any] = field(default_factory=dict)
    namespaces: list = field(default_factory=lambda: sorted(NAMESPACES))

    @contextmanager
    def tx(self, readonly=False):
        """Transaction context manager returning TxnContext."""
        from daggerml_cli.ops.base_ops import TxnContext

        txn = FakeTxn(self.kv, readonly)
        logger = logging.getLogger("fake_db")
        yield TxnContext(db=self, txn=txn, logger=logger)


@dataclass
class TmpEnv(DmlDbEnv):
    def clear_all(self):
        with self.tx(readonly=False) as txn:
            for ns in NAMESPACES:
                for obj, _ in txn.iter(ns):
                    txn.delete(obj)


@pytest.fixture(scope="class")
def temp_db():
    """Provides a temporary DmlDbEnv for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            db_env = TmpEnv.create(temp_dir, namespaces=sorted(NAMESPACES))
            yield db_env
        finally:
            db_env.clear_all()
            db_env.close()


@pytest.fixture(scope="class")
def temp_bo(temp_db):
    """Provides a BaseOps instance with a temporary database."""
    yield BaseOps(temp_db)
