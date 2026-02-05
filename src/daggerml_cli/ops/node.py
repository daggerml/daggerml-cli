"""Node operations for retrieving and inspecting DAG nodes.

This module provides NodeOps, a small helper subsystem for working with node
objects in the repository. It can retrieve a node's value one-layer deep, or
fully unroll nested Datum references into plain Python values.

Public API:
    NodeOps - Class for node inspection operations
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from daggerml_cli._db import Ref
from daggerml_cli.ops.base_ops import BaseOps
from daggerml_cli.types import Datum, DmlRepoError, Node


@dataclass
class NodeOps(BaseOps):
    """Operations for retrieving and inspecting node values."""

    def _require_node_ref(self, node_ref: Ref) -> Ref:
        if not isinstance(node_ref, Ref):
            raise DmlRepoError(f"Expected Ref, got: {type(node_ref).__name__}")
        if node_ref.nss()[0] != "node":
            raise DmlRepoError(f"Expected node ref, got: {node_ref}")
        return node_ref

    def _unroll_datum_ref(self, ref: Ref, txn, *, _stack: set[Ref] | None = None) -> Any:
        if ref.ns() == "error":
            raise DmlRepoError("Cannot unroll error value.")
        if ref.ns() != "datum":
            raise DmlRepoError(f"Expected datum ref, got: {ref}")

        stack = _stack if _stack is not None else set()
        if ref in stack:
            raise DmlRepoError(f"Cycle detected while unrolling datum: {ref}")

        stack.add(ref)
        try:
            datum: Datum = txn.get(ref)
            data = datum.data
            if isinstance(data, list):
                return [self._unroll_datum_ref(x, txn, _stack=stack) for x in data]
            if isinstance(data, dict):
                return {k: self._unroll_datum_ref(v, txn, _stack=stack) for k, v in data.items()}
            return data
        finally:
            stack.remove(ref)

    def get(self, node_ref: Ref) -> Any:
        """Retrieve node value/content one layer deep (refs preserved in collections)."""
        try:
            node_ref = self._require_node_ref(node_ref)
            with self._tx(readonly=True) as txn:
                node: Node = txn.get(node_ref)
                value_ref = node.datum_ref(txn)
                datum: Datum = txn.get(value_ref)
                return datum.data
        except Exception as e:
            raise DmlRepoError(f"Failed to get node value: {e}") from e

    def unroll(self, node_ref: Ref) -> Any:
        """Fully realize Python object without any datum refs."""
        try:
            node_ref = self._require_node_ref(node_ref)
            with self._tx(readonly=True) as txn:
                node: Node = txn.get(node_ref)
                value_ref = node.datum_ref(txn)
                return self._unroll_datum_ref(value_ref, txn)
        except Exception as e:
            raise DmlRepoError(f"Failed to unroll node value: {e}") from e
