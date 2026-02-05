from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from daggerml_cli._db import Ref
from daggerml_cli.ops.node import NodeOps
from daggerml_cli.types import Datum, DmlRepoError, ImportNode, LiteralNode
from tests.test__db import _gen_ref, scalar_strategy


def _raw_object_strategy():
    return st.recursive(
        scalar_strategy(),
        lambda children: st.one_of(
            st.lists(children, max_size=4),
            st.dictionaries(st.text(max_size=16), children, max_size=4),
        ),
        max_leaves=16,
    )


def _contains_ref(x) -> bool:
    if isinstance(x, Ref):
        return True
    if isinstance(x, list):
        return any(_contains_ref(v) for v in x)
    if isinstance(x, dict):
        return any(_contains_ref(v) for v in x.values())
    return False


def _put_datum_tree(ops, value) -> Ref:
    with ops._tx(readonly=False) as txn:

        def put_internal(v):
            if isinstance(v, list):
                return txn.put(Datum(data=[put_internal(x) for x in v]))
            if isinstance(v, dict):
                return txn.put(Datum(data={k: put_internal(vv) for k, vv in v.items()}))
            return txn.put(Datum(data=v))

        return put_internal(value)


class TestNodeOps:
    @given(obj=_raw_object_strategy())
    @settings(max_examples=25)
    def test_unroll_roundtrip(self, temp_bo, obj):
        root_datum = _put_datum_tree(temp_bo, obj)
        with temp_bo._tx(readonly=False) as txn:
            node_ref = txn.put(LiteralNode(value=root_datum))
        ops = NodeOps(_db=temp_bo._db)
        assert ops.unroll(node_ref) == obj
        assert _contains_ref(ops.unroll(node_ref)) is False

    @given(
        obj=st.one_of(
            st.lists(scalar_strategy(), max_size=4),
            st.dictionaries(st.text(max_size=16), scalar_strategy(), max_size=4),
        )
    )
    @settings(max_examples=25)
    def test_get_is_one_layer_deep(self, temp_bo, obj):
        root_datum = _put_datum_tree(temp_bo, obj)
        with temp_bo._tx(readonly=False) as txn:
            node_ref = txn.put(LiteralNode(value=root_datum))
        ops = NodeOps(_db=temp_bo._db)
        got = ops.get(node_ref)
        assert isinstance(got, type(obj))
        if isinstance(got, list):
            assert all(isinstance(x, Ref) and x.ns() == "datum" for x in got)
        else:
            assert all(isinstance(v, Ref) and v.ns() == "datum" for v in got.values())

    def test_import_node_unroll(self, temp_bo):
        with temp_bo._tx(readonly=False) as txn:
            inner_datum = txn.put(Datum(data=123))
            inner_node = txn.put(LiteralNode(value=inner_datum))
            import_node_ref = txn.put(ImportNode(dag=_gen_ref("dag"), node=inner_node))
        ops = NodeOps(_db=temp_bo._db)
        assert ops.get(import_node_ref) == 123
        assert ops.unroll(import_node_ref) == 123

    def test_requires_node_ref(self, temp_bo):
        ops = NodeOps(_db=temp_bo._db)
        with pytest.raises(DmlRepoError, match="Expected node ref"):
            ops.get(_gen_ref("datum"))
