"""DML Repository Native Implementation.

This package provides the native implementation of the DML repository system.
The public API is deliberately minimal to provide a clean, stable interface
while keeping internal implementation details private.

Public API:
    Dml - Main repository class providing complete DML functionality
    Error - Computation error representation
    DmlRepoError - Base exception for repository operations
    Deletable - Resource cleanup interface
    Ref - Reference to objects stored in repository
    Resource - External resource reference (files, URLs, etc.)
    DEFAULT_HEAD - Default branch reference
    DEFAULT_USER - Default user identifier

All other functionality is accessed through the Repo class methods.
Internal modules (repo_core, vcs, gc, dag_runtime, etc.) are implementation
details and should not be used directly.
"""

# Import the main Repo class and its dependencies
from daggerml_cli._db import Ref, Resource
from daggerml_cli.ops import DmlOps
from daggerml_cli.types import (
    DEFAULT_HEAD,
    DEFAULT_USER,
    # Constants
    Deletable,
    DmlRepoError,
    Error,
)

try:
    from daggerml_cli.__about__ import __version__
except ImportError:
    __version__ = "local"

# Make the main classes available at package level
__all__ = [
    "DmlOps",
    "Error",
    "DmlRepoError",
    "Deletable",
    "Ref",
    "Resource",
    "DEFAULT_HEAD",
    "DEFAULT_USER",
]
