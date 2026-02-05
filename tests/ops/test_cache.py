from uuid import uuid4

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from daggerml_cli._db import Ref
from daggerml_cli.ops.cache import CacheOps
from daggerml_cli.types import Argspec, ArgvNode, Dag, Datum, KwargvNode
from tests.test__db import _gen_ref, scalar_strategy


def _clear_cache_namespace(temp_bo) -> None:
    CacheOps(temp_bo._db).clear()


def _put_datum(temp_bo, data) -> Ref:
    with temp_bo._tx(readonly=False) as txn:
        return txn.put(Datum(data=data), to=_gen_ref("datum"))


def _put_argspec(temp_bo, datum_ref: Ref) -> Ref:
    with temp_bo._tx(readonly=False) as txn:
        argv_node_ref = txn.put(ArgvNode(value=datum_ref), to=_gen_ref("node", "argv"))
        kwargv_datum_ref = txn.put(Datum(data={}), to=_gen_ref("datum"))
        kwargv_node_ref = txn.put(KwargvNode(value=kwargv_datum_ref), to=_gen_ref("node", "kwargv"))
        return txn.put(Argspec(argv=argv_node_ref, kwargv=kwargv_node_ref), to=_gen_ref("argspec"))


def _put_dag(temp_bo, argspec_ref: Ref | None = None) -> Ref:
    with temp_bo._tx(readonly=False) as txn:
        return txn.put(Dag(nodes=[], names={}, result=None, argspec=argspec_ref), to=_gen_ref("dag"))


@pytest.fixture(scope="class")
def ops(temp_bo):
    return CacheOps(temp_bo._db)


class TestCacheOps:
    @given(argv_data=scalar_strategy())
    @settings(max_examples=10)
    def test_put_get_delete_roundtrip(self, ops, argv_data):
        datum_ref = _put_datum(ops, argv_data)
        argspec_ref = _put_argspec(ops, datum_ref)
        result_ref = _put_dag(ops, argspec_ref)
        _cache_ref = _gen_ref("cache")

        try:
            assert ops.get(argspec_ref) is None
            ops.put(result_ref)
            assert ops.get(argspec_ref) == result_ref
            assert ops.delete(argspec_ref) is True
            assert ops.get(argspec_ref) is None
            assert ops.delete(argspec_ref) is False
        finally:
            _clear_cache_namespace(ops)

    @given(count=st.integers(min_value=1, max_value=4))
    @settings(max_examples=10)
    def test_list_limit_and_clear(self, ops, count):
        _clear_cache_namespace(ops)

        created: list[tuple[Ref, Ref]] = []
        for _ in range(count):
            datum_ref = _put_datum(ops, uuid4().hex)
            argspec_ref = _put_argspec(ops, datum_ref)
            result_ref = _put_dag(ops, argspec_ref)
            ops.put(result_ref)
            created.append((Ref(f"cache:{argspec_ref.id()}"), result_ref))

        try:
            limited = list(ops.list(limit=1))
            limited_pairs = [(ref, entry.dag) for ref, entry in limited]
            assert len(limited_pairs) == 1
            assert limited_pairs[0] in created

            all_entries = [(ref, entry.dag) for ref, entry in ops.list()]
            assert set(all_entries) == set(created)

            assert ops.clear() == count
            assert list(ops.list()) == []
            assert ops.clear() == 0
        finally:
            _clear_cache_namespace(ops)
