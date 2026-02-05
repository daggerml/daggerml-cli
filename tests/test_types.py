"""Comprehensive tests for types.py module with Hypothesis property-based testing."""

from collections import defaultdict

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from daggerml_cli._db import Ref, Resource
from daggerml_cli.types import (
    DEFAULT_HEAD,
    NAMESPACES,
    NONE,
    Argspec,
    ArgvNode,
    Cache,
    Collection,
    Commit,
    Dag,
    Datum,
    Deletable,
    DmlBase,
    DmlRepoError,
    Error,
    FnNode,
    Head,
    ImportNode,
    Index,
    KwargvNode,
    LiteralNode,
    MaybeRefCollection,
    MaybeRefScalar,
    RefCollection,
    Scalar,
    Tree,
    _register_dml_obj,
)
from tests.test__db import REF_ALPHABET, STR_ALPHABET, _refs, resource_strategy, scalar_strategy


def _error_strategy():
    return st.builds(
        Error,
        message=st.text(alphabet=STR_ALPHABET, max_size=16),
        origin=st.text(alphabet=STR_ALPHABET, max_size=16),
        type=st.text(alphabet=STR_ALPHABET, max_size=16),
        stack=st.lists(
            st.dictionaries(
                st.text(alphabet=REF_ALPHABET, max_size=8),
                st.text(alphabet=REF_ALPHABET, max_size=8),
                max_size=3,
            ),
            max_size=3,
        ),
    )


def _dag_strategy():
    @st.composite
    def _draw_dag(draw):
        nodes = draw(st.lists(_node_ref, max_size=4))
        result = error = argspec = None
        if nodes:
            names = draw(
                st.dictionaries(
                    st.text(alphabet=REF_ALPHABET, min_size=1, max_size=8),
                    st.sampled_from(nodes),
                    max_size=4,
                )
            )
            tmp = draw(st.one_of(st.none(), st.sampled_from(nodes), _refs("error")))
            if tmp is not None and tmp.ns() == "error":
                error = tmp
            else:
                result = tmp
            argspec = draw(st.one_of(st.none(), _refs("argspec")))
        else:
            names = {}
            error = draw(st.one_of(st.none(), _refs("error")))
        return Dag(nodes=nodes, names=names, result=result, argspec=argspec, error=error)

    return _draw_dag()


def _tree_strategy():
    return st.builds(
        Tree,
        dags=st.dictionaries(
            st.text(alphabet=REF_ALPHABET, min_size=1, max_size=8),
            _refs("dag"),
            max_size=4,
        ),
    )


def _commit_strategy():
    return st.builds(
        Commit,
        parents=st.lists(_refs("commit"), max_size=3),
        tree=_refs("tree"),
        author=st.text(alphabet=REF_ALPHABET, max_size=16),
        message=st.text(alphabet=REF_ALPHABET, max_size=64),
        dag=st.one_of(st.none(), _refs("dag")),
    )


def _head_strategy():
    return st.builds(Head, commit=_refs("commit"))


def _index_strategy():
    return st.builds(Index, commit=_refs("commit"))


def _cache_strategy():
    return st.builds(Cache, dag=_refs("dag"))


def _deletable_strategy():
    return st.builds(
        Deletable,
        uri=st.text(alphabet=REF_ALPHABET + ":/", min_size=1, max_size=32),
        data=st.one_of(st.none(), _refs("datum")),
        adapter=st.one_of(st.none(), st.text(alphabet=REF_ALPHABET, min_size=1, max_size=16)),
    )


def _literal_node_strategy():
    return st.builds(LiteralNode, value=_refs("datum"))


def _argv_node_strategy():
    return st.builds(ArgvNode, value=_refs("datum"))


def _kwargv_node_strategy():
    return st.builds(KwargvNode, value=_refs("datum"))


_node_ref = st.one_of(*[_refs("node", t) for t in ["literal", "argv", "kwargv", "import", "fn"]])


def _argspec_strategy():
    return st.builds(Argspec, argv=_refs("node", "argv"), kwargv=_refs("node", "kwargv"))


def _import_node_strategy():
    return st.builds(ImportNode, dag=_refs("dag"), node=_node_ref)


def _fn_node_strategy():
    return st.builds(
        FnNode,
        dag=_refs("dag"),
        argv=st.lists(_node_ref, max_size=3),
    )


def _node_strategy():
    return st.one_of(
        _literal_node_strategy(),
        _argv_node_strategy(),
        _kwargv_node_strategy(),
        _import_node_strategy(),
        _fn_node_strategy(),
    )


def _datum_strategy():
    return st.builds(
        Datum,
        data=st.one_of(
            scalar_strategy(),
            st.lists(_refs("datum"), max_size=3),
            st.dictionaries(st.text(max_size=8), _refs("datum"), max_size=3),
        ),
    )


def _dml_obj_strategy():
    return st.one_of(
        _datum_strategy(),
        _error_strategy(),
        _argspec_strategy(),
        # _resource_strategy(),
        _deletable_strategy(),
        _dag_strategy(),
        _tree_strategy(),
        _commit_strategy(),
        _head_strategy(),
        _index_strategy(),
        _cache_strategy(),
        _node_strategy(),
    )


class TestDmlObjDecorator:
    """Test the dml_obj decorator functionality."""

    def test_register_dml_obj_registration(self):
        """Test that dml_obj decorator registers classes in NAMESPACES."""
        initial_namespaces = len(NAMESPACES)

        @_register_dml_obj
        class TestClass:
            pass

        assert "testclass" in NAMESPACES
        assert NAMESPACES["testclass"] is TestClass
        assert hasattr(TestClass, "_ns")
        assert TestClass._ns == "testclass"
        assert len(NAMESPACES) == initial_namespaces + 1

    @given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=20))
    def test_register_dml_obj_lowercase_conversion(self, class_name):
        """Test decorator converts class names to lowercase for namespace."""

        @_register_dml_obj
        class TempClass:
            pass

        # Temporarily set the class name
        TempClass.__name__ = class_name
        expected_ns = class_name.lower()

        # Re-register to test the name conversion
        obj = _register_dml_obj(TempClass)
        assert obj._ns == expected_ns


class TestDmlBase:
    """Test base class functionality."""

    def test_to_dict_excludes_private(self):
        """Test that to_dict excludes private attributes."""
        from dataclasses import dataclass

        @dataclass
        class TestClass(DmlBase):
            public: str
            _private: str = "hidden"

        obj = TestClass(public="visible", _private="hidden")
        result = obj.to_dict()
        assert "public" in result
        assert "_private" not in result
        assert result["public"] == "visible"

    @given(
        st.dictionaries(
            st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=10),
            st.one_of(st.text(max_size=20), st.integers()),
            min_size=1,
            max_size=5,
        )
    )
    def test_from_dict_creates_instance(self, field_data):
        """Test that from_dict creates correct instance with arbitrary data."""

        @_register_dml_obj
        class TestClass(DmlBase):
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)

        instance = TestClass.from_dict(field_data)
        for key, value in field_data.items():
            assert getattr(instance, key) == value


class TestResource:
    """Test Resource class validation and functionality."""

    @given(resource_strategy())
    def test_resource_creation(self, resource):
        """Test Resource creation with valid data."""
        assert isinstance(resource.uri, str)
        assert resource.data is None or isinstance(resource.data, Ref)
        assert resource.adapter is None or isinstance(resource.adapter, str)

    def test_resource_validation_uri(self):
        """Test Resource URI validation."""
        with pytest.raises(TypeError, match="uri must be str"):
            Resource(uri=123)  # type: ignore

    def test_resource_validation_data(self):
        """Test Resource data validation."""
        with pytest.raises(TypeError, match="data must be Ref or None"):
            Resource(uri="test", data="invalid")  # type: ignore

    def test_resource_validation_adapter(self):
        """Test Resource adapter validation."""
        with pytest.raises(TypeError, match="adapter must be str or None"):
            Resource(uri="test", adapter=123)  # type: ignore

    def test_resource_repr_with_adapter(self):
        """Test Resource repr with adapter."""
        resource = Resource(uri="test://uri", adapter="test_adapter")
        assert repr(resource) == "Resource(test://uri, test_adapter)"

    def test_resource_repr_without_adapter(self):
        """Test Resource repr without adapter."""
        resource = Resource(uri="test://uri", adapter=None)
        assert repr(resource) == "Resource(test://uri, None)"


class TestDataClasses:
    """Test basic data class functionality with property-based testing."""

    def test_error_from_exception(self):
        """Test Error.from_ex creates Error from exception."""
        try:
            raise ValueError("test error")
        except Exception as e:
            error = Error.from_ex(e)
            assert error.message == "test error"
            assert error.origin == "python"
            assert error.type == "valueerror"
            assert len(error.stack) > 0

    @given(_dag_strategy().filter(lambda d: d.names), _refs("node"))
    def test_dag_nameof(self, dag, node_ref):
        """Test DAG nameof method with generated data."""
        assume(node_ref not in dag.names.values())
        reverse_map = defaultdict(list)
        for name, ref in dag.names.items():
            reverse_map[ref].append(name)
        for ref, names in reverse_map.items():
            assert dag.nameof(ref) in names
        # Test with non-existent ref
        if node_ref not in dag.names.values():
            assert dag.nameof(node_ref) is None

    @given(resource_strategy())
    def test_deletable_from_resource(self, resource):
        """Test Deletable.from_resource creates deletable."""
        deletable = Deletable.from_resource(resource)
        assert deletable.uri == resource.uri
        assert deletable.adapter == resource.adapter
        assert deletable.data == resource.data
        assert isinstance(deletable, Deletable)

    @given(_dml_obj_strategy())
    def test_all_types_roundtrip(self, obj):
        """Test that all registered types can roundtrip through to_dict/from_dict."""
        obj_dict = obj.to_dict()
        restored = type(obj).from_dict(obj_dict)
        assert obj == restored

    @given(_dag_strategy().filter(lambda d: d.argspec is None))
    def test_dag_cache_key_requires_argspec(self, temp_bo, dag):
        """Test that cache_key requires argspec.

        Uses a real transaction context from the `temp_bo` fixture to exercise
        `Dag.cache_key` with a real `TxnContext` instead of a casted mock.
        """
        with pytest.raises(DmlRepoError, match="Cannot compute cache key for DAG without argspec"):
            with temp_bo._tx(readonly=True) as txn:
                dag.cache_key(txn)


class TestNodeTypes:
    """Test node type registration and serialization."""

    @given(_refs("dag"), _node_ref, _refs("datum"))
    @settings(max_examples=1)
    def test_import_node_datum_ref(self, temp_bo, dag_ref, node_ref, datum_ref):
        """ImportNode.datum_ref reads imported node value via ops."""
        node = ImportNode(dag=dag_ref, node=node_ref)
        with temp_bo._tx() as txn:
            datum_ref = txn.put(Datum(data=123), to=datum_ref)
            txn.put(LiteralNode(value=datum_ref), to=node_ref)
            assert node.datum_ref(txn) == datum_ref


class TestConstants:
    """Test module constants."""

    def test_constants_defined(self):
        """Test that required constants are defined."""
        assert NONE is not None
        assert isinstance(DEFAULT_HEAD, Ref)
        assert DEFAULT_HEAD.ns() == "head"
        assert DEFAULT_HEAD.id() == "mainbranch"

    """Test type alias definitions."""

    def test_type_aliases_importable(self):
        """Test that type aliases can be imported and used."""
        # Just test that they're importable - type checking is done by mypy
        assert Scalar is not None
        assert MaybeRefScalar is not None
        assert Collection is not None
        assert MaybeRefCollection is not None
        assert RefCollection is not None


class TestRegistries:
    """Test namespace and nodetype registries."""

    def test_registries_populated(self):
        """Test that registries contain expected entries."""
        # Check that _register_dml_obj classes are registered
        expected_namespaces = {
            "cache",
            "commit",
            "dag",
            "datum",
            "deletable",
            "error",
            "head",
            "index",
            "tree",
        }
        for namespace in expected_namespaces:
            assert namespace in NAMESPACES

    @given(st.sampled_from(list(NAMESPACES.keys())))
    def test_namespace_classes_have_ns_attribute(self, namespace):
        """Test that registered classes have correct _ns attribute."""
        cls = NAMESPACES[namespace]
        if hasattr(cls, "_ns"):
            assert cls._ns == namespace
