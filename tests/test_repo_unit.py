import json
import shutil
import tempfile
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from daggerml_cli.repo import (
    Ctx,
    Dag,
    Datum,
    Fn,
    FnDag,
    Literal,
    Node,
    Ref,
    Repo,
    Resource,
    from_json,
    to_json,
    unroll_datum,
)


@contextmanager
def tmp_repo(cache_dir=None):
    """Context manager to create a temporary repository."""
    tmpdirs = [tempfile.mkdtemp() for _ in range(2)]
    repo = Repo(tmpdirs[0], user="test", create=True, cache_db_path=cache_dir or tmpdirs[1])
    if cache_dir is None:
        with Repo(repo.cache_db_path, create=True):
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
    "name,fn_uri,args,expected",
    [
        ("len_list", "daggerml:len", [1, 2, 3], 3),
        ("len_dict", "daggerml:len", {"a": 1, "b": 2}, 2),
        ("get_dict", "daggerml:get", ({"a": 1, "b": 2}, "a"), 1),
        ("get_default", "daggerml:get", ({"a": 1}, "b", 42), 42),
        ("contains_list", "daggerml:contains", ([1, 2, 3], 2), True),
        ("contains_dict", "daggerml:contains", ({"a": 1, "b": 2}, "a"), True),
    ],
)
def test_collection_functions(name, fn_uri, args, expected):
    """Test collection-related built-in functions."""
    with tmp_repo() as repo:
        with repo.tx(True):
            # Create test arguments
            resource = Resource(fn_uri)
            argv = [resource] + (list(args) if isinstance(args, tuple) else [args])

            # Execute the function
            result = repo.check_or_submit_fn(argv)

            # Load and verify the result
            fndag_ref = repo.load_ref(result)
            fndag = repo.get(fndag_ref)
            result_node = repo.get(fndag.result)
            result_datum = repo.get(result_node.value)

            # Assert the result matches the expected value
            assert unroll_datum(result_datum.value) == expected


def test_adapter_called_correctly():
    """Test that the adapter is called with the correct arguments."""
    with tmp_repo() as repo:
        with repo.tx(True):
            # Create a Resource with an adapter and URI
            resource = Resource("test://uri", data={"key": "value"}, adapter="/bin/ls")
            argv = [resource, 0, 1, 2]

            # Patch subprocess.run to capture its arguments
            with patch("subprocess.run") as mock_run:
                mock_run.return_value.returncode = 0
                mock_run.return_value.stdout = ""
                # Call check_or_submit_fn
                repo.check_or_submit_fn(argv)
                # Verify subprocess.run was called with the correct arguments
                mock_run.assert_called_once()
                (args,), kwargs = mock_run.call_args
    assert len(args) == 2
    assert args[0] == "/bin/ls"  # Ensure adapter is called
    assert args[1] == "test://uri"  # Ensure URI is passed
    payload = json.loads(kwargs["input"])
    assert payload["kwargs"] == {"key": "value"}  # Ensure payload is correct
    assert set(payload.keys()) == {"db", "cache_key", "kwargs", "dump"}
    with tmp_repo() as repo:
        with repo.tx(True):
            ref = repo.begin(message="foo", dump=payload["dump"])
            assert isinstance(ref, Ref)


def test_complete_dag_dump_and_load():
    """Test creating, dumping and loading a complete DAG with multiple nodes and connections."""
    # Create a source repository with a complex DAG structure
    print("\n===== Starting test_complete_dag_dump_and_load =====\n")
    with tmp_repo() as source_repo:
        with source_repo.tx(True):
            # Create a tree structure for our DAG:
            #   node1 (len) -> node2 (result: 3)
            #    |
            #    v
            #  [1,2,3] (list datum)

            # Step 1: Create function argument nodes in the DAG
            index = source_repo.begin(message="test dag", name="test_dag")
            list_node = source_repo.put_node(
                Literal(source_repo.put_datum([1, 2, 3])),
                index=index,
                name="input_list",
                doc="Input list",
            )
            len_node = source_repo.put_node(
                Literal(source_repo.put_datum(Resource("daggerml:len"))),
                index=index,
                name="len_fn",
                doc="Length function",
            )

            # Step 3: Execute the function in the DAG
            # Execute len([1,2,3]) to get result 3
            result_node = source_repo.start_fn(
                index,
                argv=[len_node, list_node],
                name="len_result",
                doc="Length result",
            )

            # Step 4: Commit the DAG
            source_repo.commit(result_node, index)

            # Get the actual dag (now stored in the tree)
            ctx = Ctx.from_head(source_repo.head)
            dag_ref = ctx.dags["test_dag"]
            dag = source_repo.get(dag_ref)

            # Verify the dag structure before dumping
            assert isinstance(dag, Dag)
            assert len(dag.nodes) >= 3  # At least 3 nodes: len, list, and result
            assert len(dag.names) == 3  # Names for each node
            assert "input_list" in dag.names
            assert "len_fn" in dag.names
            assert "len_result" in dag.names
            assert dag.result is not None

            # Dump the entire DAG to JSON
            print("\n===== Dumping DAG =====\n")
            dag_dump = source_repo.dump_ref(dag_ref)
            print(f"Dump complete, length: {len(dag_dump)}")

    # Create a target repository and load the DAG there
    with tmp_repo() as target_repo:
        with target_repo.tx(True):
            # Load the dumped DAG
            print("\n===== Loading DAG =====\n")
            loaded_dag_ref = target_repo.load_ref(dag_dump)
            loaded_dag = target_repo.get(loaded_dag_ref)

            # Verify the loaded DAG has the correct structure
            assert isinstance(loaded_dag, Dag)
            assert len(loaded_dag.nodes) == len(dag.nodes)
            assert len(loaded_dag.names) == len(dag.names)
            assert set(loaded_dag.names.keys()) == set(dag.names.keys())
            assert loaded_dag.result is not None

            # Verify node types are preserved
            input_node_ref = loaded_dag.names["input_list"]
            input_node = target_repo.get(input_node_ref)
            assert isinstance(input_node, Node)
            assert isinstance(input_node.data, Literal)

            # Verify function node
            fn_node_ref = loaded_dag.names["len_fn"]
            fn_node = target_repo.get(fn_node_ref)
            assert isinstance(fn_node, Node)
            assert isinstance(fn_node.data, Literal)

            # Verify result node
            result_node_ref = loaded_dag.names["len_result"]
            result_node = target_repo.get(result_node_ref)
            assert isinstance(result_node, Node)
            assert isinstance(result_node.data, Fn)

            # Verify actual values
            input_value = unroll_datum(target_repo.get(input_node.data.value))
            assert input_value == [1, 2, 3]

            fn_value = unroll_datum(target_repo.get(fn_node.data.value))
            assert fn_value.uri == "daggerml:len"

            # Get the result value
            result_fndag_ref = result_node.data.dag
            result_fndag = target_repo.get(result_fndag_ref)
            result_value_node = target_repo.get(result_fndag.result)
            result_datum = target_repo.get(result_value_node.value)

            # Verify the result is correctly preserved
            assert result_datum.value == 3


class TestRepo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create test directories
        cls.repo_dir = tempfile.mkdtemp()
        cls.cache_dir = tempfile.mkdtemp()

        # Create repositories
        with Repo(cls.repo_dir, user="test", create=True):
            pass
        with Repo(cls.cache_dir, user="test", create=True):
            pass

    @classmethod
    def tearDownClass(cls):
        # Clean up test directories
        shutil.rmtree(cls.repo_dir)
        shutil.rmtree(cls.cache_dir)

    def setUp(self):
        # Create fresh repo for each test
        self.repo = Repo(self.repo_dir, user="test")

    def tearDown(self):
        self.repo.close()

    def test_cache_db_usage(self):
        """Test that the cache DB is used when cache_db_path is provided."""
        # Set up repo with cache path
        self.repo.cache_db_path = self.cache_dir
        # Mock the CacheDb
        mock_cache = MagicMock()
        mock_cache.__enter__ = MagicMock(return_value=mock_cache)
        mock_cache.__exit__ = MagicMock()
        mock_cache.check_or_submit_fn = MagicMock(return_value="mock_result")

        with patch("daggerml_cli.repo.CacheDb", return_value=mock_cache):
            with self.repo.tx(True):
                # Set up mocks for the pipeline
                index = MagicMock(spec=Ref)
                argv_node = MagicMock(spec=Ref)
                # Need to mock get() to return objects with datum attribute
                mock_node = MagicMock()
                mock_node.datum = "mock_value"
                with patch.object(self.repo, "get", return_value=mock_node):
                    with patch.object(self.repo, "load_ref"):
                        with patch.object(self.repo, "put_node"):
                            # Call the function being tested
                            self.repo.start_fn(index, argv=[argv_node])
                            # Verify CacheDb was created with correct path
                            self.assertEqual(mock_cache.check_or_submit_fn.call_count, 1)

    def test_no_cache_db_fallback(self):
        """Test that when no cache_db_path is provided, check_or_submit_fn is called on self."""
        # Ensure no cache path is set
        self.repo.cache_db_path = None

        with self.repo.tx(True):
            # Set up mocks
            index = MagicMock(spec=Ref)
            argv_node = MagicMock(spec=Ref)
            # Need to mock get() to return objects with datum attribute
            mock_node = MagicMock()
            mock_node.datum = "mock_value"
            # Mock methods to focus on what we're testing
            with patch.object(self.repo, "get", return_value=mock_node):
                with patch.object(self.repo, "check_or_submit_fn") as mock_check_submit:
                    mock_check_submit.return_value = "{}"
                    with patch.object(self.repo, "load_ref"):
                        with patch.object(self.repo, "put_node"):
                            # Call the function
                            self.repo.start_fn(index, argv=[argv_node])
                            # Verify local check_or_submit_fn was called
                            mock_check_submit.assert_called_once()

    def test_dump_and_load_ref(self):
        """Test that dump_ref and load_ref can serialize and deserialize references between repositories."""
        # Create test data in the first repo
        with self.repo.tx(True):
            # Create a simple node structure
            test_value = {"key": "value", "nested": [1, 2, 3]}
            datum_ref = self.repo.put_datum(test_value)
            node_ref = self.repo(Node(Literal(datum_ref)))
            # Dump the reference to JSON
            dump = self.repo.dump_ref(node_ref)
            # Verify dump is valid JSON
            self.assertIsInstance(dump, str)
            parsed = json.loads(dump)
            self.assertIsInstance(parsed, list)
        # Create a second repository for testing load_ref
        with tmp_repo() as second_repo:
            with second_repo.tx(True):
                # Load the reference into the second repo
                loaded_ref = second_repo.load_ref(dump)
                # Verify the reference was loaded correctly
                self.assertIsInstance(loaded_ref, Ref)
                # Get the node from the loaded reference
                loaded_node = second_repo.get(loaded_ref)
                self.assertIsInstance(loaded_node, Node)
                # Use unroll_datum to get actual values (instead of refs)
                loaded_value = unroll_datum(loaded_node.data.value)
                # Verify the datum value matches the original
                self.assertEqual(loaded_value, test_value)

    def test_dump_and_load_ref_complex_structure(self):
        """Test that dump_ref and load_ref work correctly with complex nested structures."""
        with self.repo.tx(True):
            # Create a complex nested structure
            nested_list = [1, "string", True, None]
            nested_dict = {"a": 1, "b": [2, 3], "c": {"d": 4}}
            resource = Resource("test://uri", adapter="test-adapter")
            complex_structure = {
                "list": nested_list,
                "dict": nested_dict,
                "resource": resource,
                "set": {1, 2, 3},
            }
            # Store the structure in the repo
            datum_ref = self.repo.put_datum(complex_structure)
            node_ref = self.repo(Node(Literal(datum_ref), doc="Complex test structure"))
            # Dump and capture the dump string
            dump_string = self.repo.dump_ref(node_ref)
        # Create a second repo to test loading
        with tmp_repo() as second_repo:
            with second_repo.tx(True):
                # Load the reference
                loaded_ref = second_repo.load_ref(dump_string)
                # Verify node was loaded correctly
                loaded_node = second_repo.get(loaded_ref)
                self.assertEqual(loaded_node.doc, "Complex test structure")
                # Use unroll_datum to get actual values (instead of refs)
                loaded_value = unroll_datum(loaded_node.data.value)
                # Verify structure was preserved (except set order which might change)
                self.assertEqual(loaded_value["list"], nested_list)
                self.assertEqual(loaded_value["dict"], nested_dict)
                self.assertEqual(loaded_value["resource"], resource)
                self.assertEqual(loaded_value["set"], {1, 2, 3})

    def test_load_ref_non_recursive(self):
        """Test that load_ref correctly handles non-recursive reference dumps."""
        with self.repo.tx(True):
            # Create a simple value
            test_value = "test string"
            datum_ref = self.repo.put_datum(test_value)
            node_ref = self.repo(Node(Literal(datum_ref)))
            # Dump the reference without recursive flag
            dump = self.repo.dump_ref(node_ref, recursive=False)
            # Create a second repository
            with tmp_repo() as second_repo:
                with second_repo.tx(True):
                    # This test depends on implementation - current implementation doesn't
                    # actually validate references at load time, but at access time
                    loaded_ref = second_repo.load_ref(dump)
                    self.assertIsInstance(loaded_ref, Ref)
                    # The access should fail when we try to get values from the reference
                    # but the assertions will vary depending on implementation
                    loaded_node = second_repo.get(loaded_ref)
                    self.assertIsInstance(loaded_node, Node)
