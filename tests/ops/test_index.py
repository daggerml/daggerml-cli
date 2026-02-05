from unittest.mock import patch

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from daggerml_cli._db import Ref, Resource
from daggerml_cli.ops.index import IndexOps
from daggerml_cli.ops.node import NodeOps
from daggerml_cli.types import (
    Argspec,
    ArgvNode,
    Commit,
    Dag,
    Datum,
    DmlRepoError,
    Error,
    FnNode,
    Head,
    ImportNode,
    Index,
    KwargvNode,
    LiteralNode,
    Tree,
)
from tests.test__db import REF_ALPHABET, _gen_ref, float_strategy, int_strategy, scalar_strategy
from tests.test_types import _index_strategy
from tests.util import REPO_ROOT

_NAME_STRAT = st.text(alphabet=REF_ALPHABET, min_size=1, max_size=12)
DELAYED_FN = Resource(str(REPO_ROOT / "tests/fn/delayed-sum.py"), adapter="dml-python-fork-adapter")
PREPOP_FN = Resource(str(REPO_ROOT / "tests/fn/prepop.py"), adapter="dml-python-fork-adapter")
ERROR_FN = Resource(str(REPO_ROOT / "tests/fn/adapter-error.py"), adapter="dml-python-fork-adapter")
RAND_FN = Resource(str(REPO_ROOT / "tests/fn/rand.py"), adapter="dml-python-fork-adapter")
SUM_FN = Resource(str(REPO_ROOT / "tests/fn/sum.py"), adapter="dml-python-fork-adapter")


def _mk_repo_state(temp_bo, *, with_argv: bool = False) -> tuple[IndexOps, Ref, Ref]:
    """Create a minimal head + working index context for IndexOps tests."""
    head_ref = _gen_ref("head")
    tree_ref = _gen_ref("tree")
    base_commit_ref = _gen_ref("commit")
    index_dag_ref = _gen_ref("dag")
    index_commit_ref = _gen_ref("commit")
    index_ref = _gen_ref("index")
    with temp_bo._tx(readonly=False) as txn:
        txn.put(Tree(dags={}), to=tree_ref)
        txn.put(Commit(parents=[], tree=tree_ref, author="test", message="base"), to=base_commit_ref)
        txn.put(Head(commit=base_commit_ref), to=head_ref)
        nodes: list[Ref] = []
        argv_node_ref: Ref | None = None
        if with_argv:
            argv_datum_ref = txn.put(Datum(data=[]), to=_gen_ref("datum"))
            argv_node_ref = txn.put(ArgvNode(value=argv_datum_ref), to=_gen_ref("node", "argv"))
            kwargv_datum_ref = txn.put(Datum(data={}), to=_gen_ref("datum"))
            kwargv_node_ref = txn.put(KwargvNode(value=kwargv_datum_ref), to=_gen_ref("node", "kwargv"))
            argspec_ref = txn.put(Argspec(argv=argv_node_ref, kwargv=kwargv_node_ref), to=_gen_ref("argspec"))
            nodes = [argv_node_ref, kwargv_node_ref]
        txn.put(Dag(nodes=nodes, names={}, result=None, argspec=(argspec_ref if with_argv else None)), to=index_dag_ref)
        txn.put(
            Commit(
                parents=[base_commit_ref],
                tree=tree_ref,
                author="test",
                message="working",
                dag=index_dag_ref,
            ),
            to=index_commit_ref,
        )
        txn.put(Index(commit=index_commit_ref), to=index_ref)
    return IndexOps(_db=temp_bo._db), head_ref, index_ref


def _unroll_datum(txn, ref: Ref):
    datum = txn.get(ref)
    assert isinstance(datum, Datum)
    data = datum.data
    if isinstance(data, list):
        return [_unroll_datum(txn, x) if isinstance(x, Ref) else x for x in data]
    if isinstance(data, dict):
        return {k: _unroll_datum(txn, v) if isinstance(v, Ref) else v for k, v in data.items()}
    return data


class TestIndexOps:
    @given(_index_strategy())
    @settings(max_examples=10)
    def test_list(self, temp_bo, idx):
        """List returns existing refs; delete removes them."""
        ops = IndexOps(_db=temp_bo._db)
        with temp_bo._tx(readonly=False) as txn:
            ref = txn.put(idx)
        try:
            assert ref in list(ops.list())
        finally:
            ops.delete(ref)
        assert ref not in list(ops.list())

    @pytest.mark.parametrize(
        "builtin,args,expected",
        [
            ("list", [1, 2], [1, 2]),
            ("dict", ["a", 1, "b", 2], {"a": 1, "b": 2}),
            ("get", [{"a": 1}, "a"], 1),
            ("get", [{"a": 1}, "b", 9], 9),
            ("contains", [{"a": 1, "b": 2}, "a"], True),
            ("contains", [{"a": 1, "b": 2}, "c"], False),
            ("contains", [[{"a": 1}, {"b": 2}], {"a": 1}], True),
            ("contains", [[{"a": 1}, {"b": 2}], {"a": 2}], False),
            ("assoc", [{"a": 1, "b": 2}, "c", 3], {"a": 1, "b": 2, "c": 3}),
            ("assoc", [{"a": 1, "b": 2}, "a", 9], {"a": 9, "b": 2}),
            ("conj", [[1, 2], 3], [1, 2, 3]),
            ("unnest", [[[1], [2, 3], [4, [5]]]], [1, 2, 3, 4, [5]]),
        ],
    )
    def test_start_fn_builtins(self, temp_bo, builtin, args, expected):
        ops, _head_ref, index = _mk_repo_state(temp_bo)
        try:
            args = [ops.put_literal(index, arg) for arg in [Resource(f"daggerml:{builtin}"), *args]]
            result = ops.start_fn(index, args)
            nv = NodeOps(_db=temp_bo._db).unroll(result)
            assert nv == expected
        finally:
            ops.delete(index)

    @given(args=st.lists(st.one_of(int_strategy(), float_strategy()), min_size=1, max_size=5))
    @settings(max_examples=10, deadline=2000)
    def test_start_fn_sum(self, temp_bo, args):
        ops, _head_ref, index = _mk_repo_state(temp_bo)
        try:
            node_args = [ops.put_literal(index, arg) for arg in [SUM_FN, *args]]
            result = ops.start_fn(index, node_args)
            nv = NodeOps(_db=temp_bo._db).unroll(result)
            assert nv == pytest.approx(sum(args))
        finally:
            ops.delete(index)

    def test_put_literal_dict_fn(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        n0 = ops.put_literal(index_ref, 0, name="v0")
        ops.put_literal(index_ref, {"a": n0}, name="v1")
        nops = NodeOps(_db=temp_bo._db)
        with ops._tx(readonly=True) as txn:
            dag: Dag = txn.get_ctx(index_ref).dag
        vals = [nops.unroll(v) for v in dag.nodes]
        vals = [str(v) if isinstance(v, dict) else v for v in vals]
        vals = [x for x in vals if not isinstance(x, Resource)]
        assert {0, "a", "{'a': 0}"} == set(vals)

    def test_put_literal_list_fn(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        n0 = ops.put_literal(index_ref, 0, name="v0")
        ops.put_literal(index_ref, [1, n0], name="v1")
        nops = NodeOps(_db=temp_bo._db)
        with ops._tx(readonly=True) as txn:
            dag: Dag = txn.get_ctx(index_ref).dag
        vals = [nops.unroll(v) for v in dag.nodes]
        vals = [tuple(v) if isinstance(v, list) else v for v in vals]
        vals = [x for x in vals if not isinstance(x, Resource)]
        assert {0, 1, (1, 0)} == set(vals)

    def test_start_fn_sum_err(self, temp_bo):
        ops, _head_ref, index = _mk_repo_state(temp_bo)
        args = [1, 2, 3, "BOGUS", 5]
        try:
            node_args = [ops.put_literal(index, arg) for arg in [SUM_FN, *args]]
            with pytest.raises(Error, match="Argument at index 3 is str, expected int or float"):
                ops.start_fn(index, node_args)
        finally:
            ops.delete(index)

    @given(
        args=st.lists(
            st.one_of(
                st.integers(min_value=-(2**63), max_value=2**63 - 1),
                st.floats(allow_nan=False, allow_infinity=False),
            ),
            max_size=6,
        )
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=2, deadline=400)
    def test_start_fn_sum_adapter(self, temp_bo, args):
        total = float(sum(args))
        ops, _head_ref, index = _mk_repo_state(temp_bo)
        try:
            fn_node = ops.put_literal(index, SUM_FN)
            arg_nodes = [ops.put_literal(index, arg) for arg in args]
            result = ops.start_fn(index, [fn_node, *arg_nodes], name="result")
            nv = NodeOps(_db=temp_bo._db).unroll(result)
            assert nv == pytest.approx(total)
        finally:
            ops.delete(index)

    @given(
        args=st.lists(
            st.one_of(
                st.integers(min_value=-(2**63), max_value=2**63 - 1),
                st.floats(allow_nan=False, allow_infinity=False),
            ),
            max_size=6,
        ),
        prepop=st.floats(allow_nan=False, allow_infinity=False),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=2, deadline=400)
    def test_start_fn_prepop(self, temp_bo, args, prepop):
        total = float(sum(args) * prepop)
        ops, _head_ref, index = _mk_repo_state(temp_bo)
        try:
            fn_node = ops.put_literal(index, PREPOP_FN)
            arg_nodes = [ops.put_literal(index, arg) for arg in args]
            prepop_node = ops.put_literal(index, prepop)
            result = ops.start_fn(index, [fn_node, *arg_nodes], kwargv={"x": prepop_node}, name="result")
            nv = NodeOps(_db=temp_bo._db).unroll(result)
            assert nv == pytest.approx(total)
        finally:
            ops.delete(index)

    @given(
        args=st.lists(
            st.one_of(
                st.integers(min_value=-(2**63), max_value=2**63 - 1),
                st.floats(allow_nan=False, allow_infinity=False),
            ),
            max_size=6,
        )
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=2, deadline=400)
    def test_start_fn_delayed_sum_adapter(self, temp_bo, args):
        total = float(sum(args))
        ops, _head_ref, index = _mk_repo_state(temp_bo)
        try:
            with patch.dict(__import__("os").environ, DML_TMP_DIR=ops._db.path):
                fn_node = ops.put_literal(index, DELAYED_FN)
                arg_nodes = [ops.put_literal(index, arg) for arg in args]
                # First call returns None (job not done yet)
                result = ops.start_fn(index, [fn_node, *arg_nodes], name="result")
                assert result is None
                # Second call returns the result
                result = ops.start_fn(index, [fn_node, *arg_nodes], name="result")
                assert result is not None
                nv = NodeOps(_db=temp_bo._db).unroll(result)
                assert nv == pytest.approx(total)
        finally:
            ops.delete(index)

    def test_start_fn_caching(self, temp_bo):
        # ensure clean DB for this test to prevent map growth from prior tests
        temp_bo._db.clear_all()
        ops, _head_ref, index = _mk_repo_state(temp_bo)
        try:
            fn_node = ops.put_literal(index, RAND_FN)
            # First call generates a random UUID
            result1 = ops.start_fn(index, [fn_node], name="result1")
            nv1 = NodeOps(_db=temp_bo._db).unroll(result1)
            # Second call with same args should return cached result
            result2 = ops.start_fn(index, [fn_node], name="result2")
            nv2 = NodeOps(_db=temp_bo._db).unroll(result2)
            assert nv1 == nv2
            assert isinstance(nv1, str)
            assert len(nv1) == 36
            assert nv1.count("-") == 4
        finally:
            ops.delete(index)

    def test_start_fn_no_caching(self, temp_bo):
        # ensure clean DB to avoid map growth between runs
        temp_bo._db.clear_all()
        ops, _head_ref, index = _mk_repo_state(temp_bo)
        try:
            fn_node = ops.put_literal(index, RAND_FN)
            # First call with cache=False generates a random UUID
            result1 = ops.start_fn(index, [fn_node], name="result1", cache=False)
            nv1 = NodeOps(_db=temp_bo._db).unroll(result1)
            # Second call with cache=False should return a different UUID
            result2 = ops.start_fn(index, [fn_node], name="result2", cache=False)
            nv2 = NodeOps(_db=temp_bo._db).unroll(result2)
            assert nv1 != nv2
            assert isinstance(nv1, str)
            assert isinstance(nv2, str)
            assert len(nv1) == 36
            assert nv1.count("-") == 4
            assert len(nv2) == 36
            assert nv2.count("-") == 4
        finally:
            ops.delete(index)

    @given(value=scalar_strategy(), name=_NAME_STRAT)
    @settings(max_examples=10)
    def test_put_literal(self, temp_bo, value, name):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            node_ref = ops.put_literal(index_ref, value, name=name)
            with ops._tx(readonly=True) as txn:
                ctx = txn.get_ctx(index_ref)
                assert node_ref in ctx.dag.nodes
                assert ctx.dag.names[name] == node_ref
                node = txn.get(node_ref)
                assert isinstance(node, LiteralNode)
                assert _unroll_datum(txn, node.value) == value
        finally:
            ops.delete(index_ref)

    def test_get_argv_raises_when_missing(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo, with_argv=False)
        try:
            with pytest.raises(DmlRepoError, match="DAG has no argv node"):
                ops.get_argv(index_ref)
        finally:
            ops.delete(index_ref)

    def test_get_argv_returns_node_when_present(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo, with_argv=True)
        try:
            argv_node_ref = ops.get_argv(index_ref)
            with ops._tx(readonly=True) as txn:
                node = txn.get(argv_node_ref)
                assert isinstance(node, ArgvNode)
        finally:
            ops.delete(index_ref)

    def test_put_import_incomplete_dag_errors(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            with ops._tx(readonly=True) as txn:
                ctx = txn.get_ctx(index_ref)
                with pytest.raises(DmlRepoError, match="Cannot import from a DAG with no result node"):
                    ops.put_import(index_ref, ctx.commit.dag)
        finally:
            ops.delete(index_ref)

    def test_put_import_imports_other_dag_result(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            other_dag_ref = _gen_ref("dag")
            other_node_ref = _gen_ref("node", "literal")
            other_datum_ref = _gen_ref("datum")
            with ops._tx(readonly=False) as txn:
                txn.put(Datum(data=123), to=other_datum_ref)
                txn.put(LiteralNode(value=other_datum_ref), to=other_node_ref)
                txn.put(Dag(nodes=[other_node_ref], names={}, result=other_node_ref, argspec=None), to=other_dag_ref)

            imported_ref = ops.put_import(index_ref, other_dag_ref, name="imported")
            with ops._tx(readonly=True) as txn:
                node = txn.get(imported_ref)
                assert isinstance(node, ImportNode)
                assert node.dag == other_dag_ref
                assert node.node == other_node_ref
                ctx = txn.get_ctx(index_ref)
                assert imported_ref in ctx.dag.nodes
        finally:
            ops.delete(index_ref)

    @given(a=scalar_strategy(), b=scalar_strategy())
    @settings(max_examples=10)
    def test_start_fn_builtin_list(self, temp_bo, a, b):
        # use a clean DB per Hypothesis example to avoid map growth
        temp_bo._db.clear_all()
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            fn_ref = ops.put_literal(index_ref, Resource("daggerml:list"))
            a_ref = ops.put_literal(index_ref, a)
            b_ref = ops.put_literal(index_ref, b)
            argv = [fn_ref, a_ref, b_ref]
            result_ref = ops.start_fn(index_ref, argv, name="result")
            assert result_ref is not None
            with ops._tx(readonly=True) as txn:
                node = txn.get(result_ref)
                assert isinstance(node, FnNode)
                assert node.argv == argv
                value_ref = txn.get(result_ref).datum_ref(txn)
                assert _unroll_datum(txn, value_ref) == [a, b]
        finally:
            ops.delete(index_ref)

    def test_start_fn_requires_resource_first_arg(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        non_resource = ops.put_literal(index_ref, 123)
        with pytest.raises(DmlRepoError, match="First arg must be Resource"):
            ops.start_fn(index_ref, [non_resource])

    @given(value=scalar_strategy(), dag_name=_NAME_STRAT)
    @settings(max_examples=10)
    def test_commit_deletes_index_and_updates_head(self, temp_bo, value, dag_name):
        ops, head_ref, index_ref = _mk_repo_state(temp_bo)
        with ops._tx(readonly=True) as txn:
            before = txn.get_ctx(head_ref).head.commit
        node_ref = ops.put_literal(index_ref, value, name="result")
        commit_ref = ops.commit(index_ref, node_ref, message="done", dag_name=dag_name, head=head_ref)

        with ops._tx(readonly=True) as txn:
            assert not txn.exists(index_ref)
            assert txn.get_ctx(head_ref).head.commit == commit_ref
            assert txn.get_ctx(head_ref).head.commit != before

            commit_obj = txn.get(commit_ref)
            assert isinstance(commit_obj, Commit)
            assert commit_obj.message == "done"
            tree_obj = txn.get(commit_obj.tree)
            assert isinstance(tree_obj, Tree)
            assert dag_name in tree_obj.dags
