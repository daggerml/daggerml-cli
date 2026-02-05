"""Comprehensive tests for base_ops.py module with real database testing."""

from tempfile import TemporaryDirectory

import pytest
from hypothesis import given

from daggerml_cli._db import DmlDbEnv
from daggerml_cli.ops.base_ops import BaseOps
from daggerml_cli.types import NAMESPACES, Datum
from tests.test__db import _gen_ref, dml_object
from tests.test_types import DmlRepoError, _dml_obj_strategy


class TestBaseOps:
    """Test BaseOps functionality."""

    @given(_dml_obj_strategy())
    def test_putget_roundtrip(self, temp_bo, obj):
        """Test successful private _get operation."""
        with temp_bo._tx(readonly=False) as ctx:
            ref = ctx.put(obj)
        with temp_bo._tx(readonly=True) as ctx:
            retrieved_obj = ctx.get(ref)
        assert retrieved_obj == obj
        with temp_bo._tx(readonly=False) as ctx:
            ctx.delete(ref)

    @given(dml_object())
    def test_dump_load_roundtrip(self, obj):
        """Test successful private _dump and _load operations."""

        def insert(txn, x):
            ## recursively insert object as Datums and return Ref.
            if isinstance(x, (tuple, list)):
                x = [insert(txn, item) for item in x]
            elif isinstance(x, dict):
                x = {k: insert(txn, v) for k, v in x.items()}
            return txn.put(Datum(x))

        def get(txn, ref):
            ## recursively get object from Ref.
            obj = txn.get(ref).data
            if isinstance(obj, list):
                return [get(txn, item) for item in obj]
            elif isinstance(obj, dict):
                return {k: get(txn, v) for k, v in obj.items()}
            return obj

        with TemporaryDirectory() as temp_dir:
            new_db = DmlDbEnv.create(temp_dir, namespaces=sorted(NAMESPACES))
            with BaseOps(new_db)._tx(readonly=False) as ctx:
                ref = insert(ctx, obj)
                dump_payload = ctx.dump(ref)
        assert isinstance(dump_payload, str)
        with TemporaryDirectory() as temp_dir:
            new_db = DmlDbEnv.create(temp_dir, namespaces=sorted(NAMESPACES))
            with BaseOps(new_db)._tx(readonly=False) as ctx:
                loaded_ref = ctx.load(dump_payload)
                loaded_obj = get(ctx, loaded_ref)
        assert loaded_obj == obj

    @given(_dml_obj_strategy())
    def test_delete(self, temp_bo, obj):
        """Test successful private _delete operation."""
        with temp_bo._tx(readonly=False) as ctx:
            ref = ctx.put(obj)
        with temp_bo._tx(readonly=False) as ctx:
            ctx.delete(ref)
        with temp_bo._tx(readonly=True) as ctx:
            with pytest.raises(DmlRepoError, match="Object not found:"):
                ctx.get(ref)

    @given(_dml_obj_strategy())
    def test_iter(self, temp_bo, obj):
        """Test successful private _get operation."""
        with temp_bo._tx(readonly=False) as ctx:
            ref = ctx.put(obj)
        with temp_bo._tx(readonly=True) as ctx:
            assert [ref] == list(ctx.iter(ref.ns()))
        temp_bo._db.clear_all()

    @given(_dml_obj_strategy())
    def test_exists(self, temp_bo, obj):
        """Test successful private _get operation."""
        with temp_bo._tx(readonly=False) as ctx:
            ref = ctx.put(obj)
        with temp_bo._tx(readonly=True) as ctx:
            assert ctx.exists(ref)
        with temp_bo._tx(readonly=False) as ctx:
            ctx.delete(ref)
        with temp_bo._tx(readonly=True) as ctx:
            assert not ctx.exists(ref)
        temp_bo._db.clear_all()

    def test_get_error(self, temp_bo):
        """Test private _get operation with error."""
        with pytest.raises(DmlRepoError, match="Object not found:"):
            with temp_bo._tx(readonly=True) as ctx:
                ctx.get(_gen_ref("head"))
