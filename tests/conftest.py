"""Common test fixtures for dml-util tests."""

import os
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def setup_envvars():
    with patch.dict(os.environ):
        # Clear AWS environment variables before any tests run
        for k in os.environ:
            if k.startswith("AWS_") or k.startswith("DML_"):
                del os.environ[k]
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        os.environ["PYTHONPATH"] = "."  # ensure `tests` is in PYTHONPATH
        yield
