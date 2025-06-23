import json
import os
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from daggerml_cli import api
from daggerml_cli.config import Config
from daggerml_cli.repo import (
    Error,
    FnDag,
    Node,
    Ref,
    Resource,
    deserialize_resource,
    serialize_resource,
)
from daggerml_cli.util import assoc, conj
from tests.util import SimpleApi

SUM = Resource("./tests/fn/sum.py", adapter="dml-python-fork-adapter")


def env(**kwargs):
    return mock.patch.dict(os.environ, **kwargs)


class TestApiCreate(TestCase):
    def test_create_dag(self):
        with TemporaryDirectory() as tmpd0, TemporaryDirectory() as tmpd1:
            ctx = Config(
                _CONFIG_DIR=tmpd0,
                _PROJECT_DIR=tmpd1,
                _USER="user0",
            )
            api.create_repo(ctx, "test")
            assert api.with_query(api.list_repo, "[*].name")(ctx) == ["test"]
            api.config_repo(ctx, "test")
            assert api.jsdata(api.list_branch(ctx)) == ["head/main"]
            api.create_branch(ctx, "b0")
            assert api.current_branch(ctx) == "b0"


class TestApiBase(TestCase):
    def tmpd(self):
        return TemporaryDirectory(prefix=f"dml.{self.id()}")

    def test_create_dag(self):
        with self.tmpd() as config_dir:
            with SimpleApi.begin("d0", config_dir=config_dir, cache_path=config_dir) as d0:
                data = {"foo": 23, "bar": {4, 6}, "baz": [True, 3]}
                n0 = d0.put_literal(data, name="n0", doc="This is my data.")
                with d0.tx():
                    assert d0.get_node("n0") == n0
                    assert n0().doc == "This is my data."
                d0.commit(n0)
                d0.test_close(self)
            with SimpleApi.begin("d1", config_dir=config_dir, cache_path=config_dir) as d1:
                n0 = d1.put_load("d0", name="n0", doc="From dag d0.")
                with d1.tx():
                    assert d1.get_node("n0") == n0
                    assert n0().doc == "From dag d0."
                n1 = d1.put_literal([n0, n0, 2])
                assert d1.unroll(n1) == [data, data, 2]
                d1.commit(n1)
                d1.test_close(self)

    def test_name(self):
        with SimpleApi.begin() as d0:
            n0 = d0.put_literal(42)
            d0.set_node("n0", n0)
            assert d0.get_node("n0") == n0
            d0.test_close(self)

    def test_fn(self):
        with SimpleApi.begin() as d0:
            result = d0.start_fn(SUM, 1, 2, name="result", doc="I called a func!")
            with d0.tx():
                assert d0.get_node("result") == result
                with self.assertRaises(Error):
                    d0.get_node("BOGUS")
                assert result().doc == "I called a func!"
            assert d0.unroll(result)[1] == 3
            d0.test_close(self)

    def test_fn_load_names(self):
        with self.tmpd() as config_dir:
            with SimpleApi.begin("d0", config_dir=config_dir) as d0:
                foo = d0.start_fn(SUM, 1, 2, name="foo")
                d0.put_literal("x", name="bar")
                with d0.tx():
                    assert d0.get_node("foo") == foo
                    assert isinstance(foo(), Node)
                    ln = d0.get_dag(foo)
                    assert isinstance(ln(), FnDag)
                    uuid = d0.get_node(dag=ln, name="uuid")
                    nv = d0.unroll(uuid)
                    assert isinstance(nv, str)
                d0.commit(foo)
                assert d0.unroll(foo)[1] == 3
                d0.test_close(self)

            with SimpleApi.begin("d1", config_dir=config_dir) as d1:
                d0_node = d1.put_load("d0")
                d0_dag = d1.get_dag(d0_node)
                foo_node = d1.get_node("foo", d0_dag)
                assert d1.unroll(d1.get_node("bar", d0_dag)) == "x"
                assert d1.unroll(foo_node)[1] == 3
                nd1 = d1.put_load(d0_dag, foo_node)
                assert d1.unroll(nd1)[1] == 3
                with d1.tx():
                    assert nd1().data.dag == d0_dag
                nd_foo_dag = d1.get_dag(nd1, recurse=True)
                nd_foo_node = d1.get_node(dag=nd_foo_dag, name="uuid")
                nd_foo_uuid = d1.put_load(nd_foo_dag, nd_foo_node)
                assert d1.unroll(nd_foo_uuid) == d1.unroll(d0_node)[0]
                d0.test_close(self)

    def test_fn2(self):
        with SimpleApi.begin() as d0:
            result = d0.start_fn(SUM, 1, 2, name="my-fn", doc="I called a func!")
            with d0.tx():
                assert d0.get_node("my-fn") == result
                with self.assertRaises(Error):
                    d0.get_node("BOGUS")
                assert result().doc == "I called a func!"
            assert d0.unroll(result)[1] == 3
            d0.commit(result)
            d0.test_close(self)

    def test_repo_cache(self):
        argv = [SUM, 1, 2]
        with SimpleApi.begin() as d0:
            res0 = d0.unroll(d0.start_fn(*argv))
            res1 = d0.unroll(d0.start_fn(*argv))
            assert res0 == res1
            assert res0[1] == 3
            d0.test_close(self)

    def test_cross_repo_cache(self):
        argv = [SUM, 1, 2]

        with self.tmpd() as cache_path:
            with SimpleApi.begin(cache_path=cache_path) as d0:
                res0 = d0.unroll(d0.start_fn(*argv))
                d0.test_close(self)

            with SimpleApi.begin(cache_path=cache_path) as d0:
                res1 = d0.unroll(d0.start_fn(*argv))
                d0.test_close(self)

        assert res0 == res1

    def test_retry(self):
        argv = [SUM, 1, 2]

        with env(DML_FN_FILTER_ARGS="True"):
            with self.tmpd() as cache_path:
                with SimpleApi.begin(cache_path=cache_path) as d0:
                    res0 = d0.unroll(d0.start_fn(*argv))
                    d0.test_close(self)
                cache = api.list_cache(d0.ctx)
                assert len(cache) == 1
                assert cache[0].keys() == {"cache_key", "dag_id"}
                api.delete_cache(d0.ctx, cache[0]["cache_key"])

                with SimpleApi.begin(cache_path=cache_path) as d0:
                    res1 = d0.unroll(d0.start_fn(*argv))
                    d0.test_close(self)

        assert res0 != res1
        assert res0[1] == 3

    def test_serde(self):
        data = {"x": Resource("u", adapter="bar")}
        js = json.dumps(data, default=serialize_resource)
        d2 = json.loads(js, object_hook=deserialize_resource)
        assert data == d2

    # def test_fn_logs(self):
    #     argv = [SUM, 1, 2]
    #
    #     with SimpleApi.begin() as d0:
    #         res = d0.start_fn(*argv)
    #         with d0.tx():
    #             res = d0.get_dag(res)
    #         desc = api.describe_dag(d0.ctx, res)
    #         d0.test_close(self)
    #     assert desc["logs"] == {"foo": "bar"}

    def test_cached_errors(self):
        argv = [SUM, 1, 2, "BOGUS"]
        with self.tmpd() as cache_path:
            with env(DML_NO_CLEAN="1"):
                with SimpleApi.begin(cache_path=cache_path) as d0:
                    with self.assertRaises(Error):
                        d0.start_fn(*argv)
            with env(DML_FN_FILTER_ARGS="True", DML_NO_CLEAN="1"):
                with self.assertRaises(Error):
                    with SimpleApi.begin(cache_path=cache_path) as d0:
                        d0.start_fn(*argv)

    def test_resource(self):
        with SimpleApi.begin() as d0:
            resource = Resource("uri:here", data={"a": 1, "b": [2, 3], "c": Resource("qwer")})
            d0.put_literal(resource)

    def test_specials(self):
        with SimpleApi.begin() as d0:

            def check_len(n, v):
                assert d0.unroll(d0.len(n)) == len(v)

            def check_keys(n, v):
                assert d0.unroll(d0.keys(n)) == sorted(v.keys())

            def check_contains(n, v):
                for i in v:
                    assert d0.unroll(d0.contains(n, d0.put_literal(i)))
                assert not d0.unroll(d0.contains(n, d0.put_literal("BOGUS")))

            def check_list_get(n, v):
                for i in range(len(v)):
                    assert d0.unroll(d0.get(n, d0.put_literal(i))) == v[i]
                len_v = d0.put_literal(len(v))
                with self.assertRaisesRegex(Error, "list index out of range"):
                    d0.get(n, len_v)

            def check_slice(n, v, *k):
                assert d0.unroll(d0.get(n, d0.put_literal(list(k)))) == v[slice(*k)]

            def check_dict_get(n, v):
                for i in v:
                    assert d0.unroll(d0.get(n, d0.put_literal(i)))
                with self.assertRaises(Error):
                    d0.unroll(d0.get(n, d0.put_literal("BOGUS")))

            def check_assoc(n, v, k, x):
                assert d0.unroll(d0.assoc(n, k, x)) == assoc(v, k, x)

            def check_conj(n, v, x):
                assert d0.unroll(d0.conj(n, x)) == conj(v, x)

            x0 = {
                "list": [1, 2, 3],
                "set": {1, 2, 3},
                "dict": {"x": 1, "y": 2, "z": 3},
                "int": 0,
                "float": 0.1,
                "bool": True,
                "NoneType": None,
                "Resource": Resource("test"),
            }
            n0 = d0.put_literal(x0)
            for k, v in x0.items():
                n = d0.get(n0, d0.put_literal(k), name="n", doc="a node")
                with d0.tx():
                    assert n().doc == "a node"
                assert d0.unroll(n) == v
                assert d0.unroll(d0.type(n)) == k
                if k == "list":
                    check_len(n, v)
                    check_list_get(n, v)
                    check_contains(n, v)
                    check_conj(n, v, 4)
                    check_assoc(n, v, 0, 0)
                    check_slice(n, v, 1, None, None)
                elif k == "set":
                    check_len(n, v)
                    check_contains(n, v)
                    check_conj(n, v, 4)
                elif k == "dict":
                    check_len(n, v)
                    check_keys(n, v)
                    check_dict_get(n, v)
                    check_contains(n, v)
                    check_assoc(n, v, "x", 0)

            assert d0.unroll(d0.list()) == []
            assert d0.unroll(d0.list(0, 1, 2, 3)) == [0, 1, 2, 3]

            assert d0.unroll(d0.dict()) == {}
            assert d0.unroll(d0.dict("x", 1, "y", 2, "z", 3)) == {
                "x": 1,
                "y": 2,
                "z": 3,
            }

            assert d0.unroll(d0.set()) == set()
            assert d0.unroll(d0.set(0, 1, 2, 3)) == {0, 1, 2, 3}

            assert d0.unroll(d0.build([1, {"x": 42}], d0.put_literal(42))) == [
                1,
                {"x": 42},
            ]
            literal = d0.put_literal(42)
            d0.commit(literal)
            d0.test_close(self)

    def test_describe_dag(self):
        with self.tmpd() as config_dir:
            with SimpleApi.begin("d0", config_dir=config_dir) as d0:
                d0.commit(d0.put_literal(23))
                # d0.test_close(self)
            with SimpleApi.begin("d1", config_dir=config_dir) as d1:
                nodes = [
                    d1.put_literal(SUM),
                    d1.put_load("d0"),
                    d1.put_literal(13),
                ]
                result = d1.start_fn(*nodes)
                assert d1.unroll(result)[1] == 36
                d1.commit(result)
                # d1.test_close(self)
            (ref,) = (x.id for x in api.list_dags(d1.ctx) if x.name == "d1")
            desc = api.describe_dag(d1.ctx, Ref(f"dag/{ref}"))
            # assert [x["type"] for x in desc["edges"]] is None
            for edge in desc["edges"]:
                if edge["type"] == "dag":
                    assert edge["source"] in {x["id"] for x in desc["nodes"]}
                else:
                    # assert edge["source"] in {x["id"] for x in desc["nodes"]}
                    assert edge["target"] in {x["id"] for x in desc["nodes"]}
            self.assertCountEqual(
                [x["node_type"] for x in desc["nodes"]],
                ["literal", "literal", "import", "fn"],
            )
            self.assertCountEqual(
                [x["data_type"] for x in desc["nodes"]],
                ["resource", "int", "int", "list"],
            )
            assert len(desc["edges"]) == len(nodes) + 2  # +1 because dag->node edge
            assert {e["source"] for e in desc["edges"] if e["type"] == "node"} == {x for x in nodes}

    def test_describe_dag_w_errs(self):
        with SimpleApi.begin("d0") as d0:
            nodes = [
                d0.put_literal(SUM),
                d0.put_literal(1),
                d0.put_literal("BOGUS"),
            ]
            with self.assertRaises(Error):
                d0.start_fn(*nodes, name="bogus-fn")
            d0.commit(d0.put_literal(None))
            descs = d0.test_close(self)
        (desc,) = [x for x in descs if x["argv"] is None]
        self.assertCountEqual(
            [x["name"] for x in desc["nodes"] if x["name"] is not None],
            ["bogus-fn"],
        )
        self.assertCountEqual(
            [x["node_type"] for x in desc["nodes"]],
            ["literal", "literal", "literal", "literal", "fn"],
        )
        self.assertCountEqual(
            [x["data_type"] for x in desc["nodes"]],
            ["resource", "int", "str", "error", "nonetype"],
        )
