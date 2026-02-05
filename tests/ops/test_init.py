from hypothesis import given, settings
from hypothesis import strategies as st

from daggerml_cli._db import Ref
from daggerml_cli.ops import DmlOps
from tests.test_types import STR_ALPHABET, _refs

# Simple strategy for user and refs
user_strategy = st.text(alphabet=STR_ALPHABET, min_size=1, max_size=8)
ref_strategy = st.builds(Ref, st.text(alphabet=STR_ALPHABET, min_size=1, max_size=16))


class TestDml:
    @given(user_strategy)
    @settings(max_examples=3)
    def test_temporary_context_manager(self, user):
        with DmlOps.temporary(user) as repo:
            assert isinstance(repo, DmlOps)
            assert repo._db is not None
            assert hasattr(repo, "dag")
            assert hasattr(repo, "gc")

    @given(user_strategy, _refs("head"))
    @settings(max_examples=3)
    def test_init_and_properties(self, user, ref):
        with DmlOps.temporary(user) as repo:
            # Properties should return correct types
            assert hasattr(repo, "dag")
            assert hasattr(repo, "gc")
            assert hasattr(repo, "commit")
            assert hasattr(repo, "head")
            assert hasattr(repo, "index")
            assert hasattr(repo, "node")
            assert hasattr(repo, "cache")
