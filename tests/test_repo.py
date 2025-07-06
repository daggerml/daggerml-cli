import json
import shutil
import tempfile
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from daggerml_cli.repo import Literal, Node, Ref, Repo, Resource, unroll_datum


@contextmanager
def tmp_repo(cache_path=None):
    """Context manager to create a temporary repository."""
    tmpdirs = [tempfile.mkdtemp() for _ in range(2)]
    repo = Repo(tmpdirs[0], user="test", create=True, cache_path=cache_path or tmpdirs[1])
    if cache_path is None:
        with Repo(repo.cache_path, create=True):
            pass
    try:
        yield repo
    finally:
        repo.close()
        for tmpd in tmpdirs:
            shutil.rmtree(tmpd)


@pytest.mark.parametrize(
    "name,test_value",
    [
        ("simple_string", "test string"),
        ("simple_int", 42),
        ("simple_float", 3.14159),
        ("simple_none", None),
        ("simple_bool", True),
        ("simple_list", [1, "string", True, None]),
        ("simple_dict", {"a": 1, "b": 2, "c": 3}),
        ("simple_set", {1, 2, 3}),
        ("resource", Resource("test://uri", adapter="test-adapter")),
        (
            "nested_structure",
            {
                "list": [1, "string", True, None],
                "dict": {"a": 1, "b": [2, 3], "c": {"d": 4}},
                "resource": Resource("test://uri", adapter="test-adapter"),
                "set": {1, 2, 3},
            },
        ),
    ],
)
def test_dump_and_load(name, test_value):
    """Parameterized test for dump_ref and load_ref with different data types."""
    # Create two independent repositories from the factory
    with tmp_repo() as repo:
        # Store the test value in the source repo
        with repo.tx(True):
            datum_ref = repo.put_datum(test_value)
            node_ref = repo(Node(Literal(datum_ref), doc=f"Test {name}"))
            dump = repo.dump_ref(node_ref)

    with tmp_repo() as repo:
        # Load in the target repo
        with repo.tx(True):
            loaded_ref = repo.load_ref(dump)
            assert isinstance(loaded_ref, Ref)

            loaded_node = repo.get(loaded_ref)
            assert isinstance(loaded_node, Node)
            assert loaded_node.doc == f"Test {name}"

            # Get the actual value using unroll_datum
            loaded_value = unroll_datum(loaded_node.data.value)
            assert loaded_value == test_value


@pytest.mark.parametrize(
    "op,args,expected",
    [
        ("get", ({"a": 1, "b": 2}, "a"), 1),
        ("get", ({"a": 1}, "b", 42), 42),
        ("get", (["a", "b", "c"], 1), "b"),
        ("get", (list("abcde"), [1, 3]), ["b", "c"]),  # slice
        ("contains", ([1, 2, 3], 2), True),
        ("contains", ({"a": 1, "b": 2}, "a"), True),
        ("contains", ([1, 2, 3], "a"), False),
        ("contains", ({"a": 1, "b": 2}, "x"), False),
        ("list", (1, 2, 3), [1, 2, 3]),
        ("dict", ("a", 1, "b", 2), {"a": 1, "b": 2}),
        ("set", (1, 2, 3, 2), {1, 2, 3}),
        ("assoc", ({"a": 1}, "b", 2), {"a": 1, "b": 2}),
        ("conj", ([1, 2], 3), [1, 2, 3]),
    ],
)
def test_start_fn_with_builtins(op, args, expected):
    """Test start_fn with built-in functions using patched methods."""
    argv = [Resource(f"daggerml:{op}")] + (list(args) if isinstance(args, tuple) else [args])
    with tmp_repo() as repo:
        with repo.tx(True):
            dag = repo.begin(message="test dag", name="test")
            argvs = [repo.put_node(Literal(repo.put_datum(arg)), index=dag) for arg in argv]
            result = repo.start_fn(index=dag, argv=argvs)
            assert unroll_datum(result().value) == expected


def test_adapter_called_correctly():
    """Test start_fn with built-in functions using patched methods."""
    argv = [Resource("foo://bar", data={"a": "b"}, adapter="ls"), 1, 2, 3]
    with tmp_repo() as repo:
        with repo.tx(True):
            dag = repo.begin(message="test dag", name="test")
            argvs = [repo.put_node(Literal(repo.put_datum(arg)), index=dag) for arg in argv]
            with patch("subprocess.run") as mock_run:
                mock_run.return_value.returncode = 0
                mock_run.return_value.stdout = ""
                mock_run.return_value.stderr = "my cool stderr"
                repo.start_fn(index=dag, argv=argvs)
                # Verify subprocess.run was called with the correct arguments
                mock_run.assert_called_once()
                (args,), kwargs = mock_run.call_args
    assert len(args) == 2
    assert args[0] == shutil.which(argv[0].adapter)  # Ensure correct adapter is called
    assert args[1] == argv[0].uri  # Ensure correct URI is passed
    payload = json.loads(kwargs["input"])
    assert set(payload.keys()) == {"cache_path", "cache_key", "kwargs", "dump"}
    assert payload["kwargs"] == argv[0].data
    assert payload["cache_path"] == repo.cache_path
    assert isinstance(payload["cache_key"], str)
    assert isinstance(payload["dump"], str)
    # check to ensure the dump is loadable
    with tmp_repo() as repo:
        with repo.tx(True):
            ref = repo.begin(message="foo", dump=payload["dump"])
            assert isinstance(ref, Ref)
            assert unroll_datum(ref().dag().argv().value) == argv
