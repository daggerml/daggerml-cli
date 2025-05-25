from daggerml_cli.repo import Fn, FnCache, FnDag, Import, Resource
from daggerml_cli.util import assoc, flatten, tree_map


def make_node(name, ref):
    node = ref()
    val = node.value
    data_type = type(node.error if val is None else val().value)
    return {
        "id": ref,
        "name": name,
        "doc": node.doc,
        "node_type": type(node.data).__name__.lower(),
        "data_type": data_type.__name__.lower(),
    }


def make_edges(ref):
    node = ref()
    out = []
    if isinstance(node.data, Import):
        out.append({"source": ref, "target": node.data.dag, "type": "dag"})
    if isinstance(node.data, Fn):
        out.extend([{"source": x, "target": ref, "type": "node"} for x in node.data.argv])
    return out


def filter_edges(topology):
    def valid(x):
        return x["type"] == "dag" or {x["source"], x["target"]} < nodes

    nodes = {x["id"] for x in topology["nodes"]}
    return assoc(topology, "edges", list(filter(valid, topology["edges"])))


def get_logs(dag):
    logs = getattr(dag, "logs", None)
    if logs is None:
        return
    from daggerml_cli.repo import unroll_datum

    logs = tree_map(lambda x: isinstance(x, Resource), lambda x: x.uri, unroll_datum(logs))
    return logs


def topology(db, ref):
    dag = ref()
    cache = None
    if isinstance(dag, FnDag):
        fncache = db(FnCache(dag.argv, None), return_existing=True)
        cache = {
            "is_cached": fncache().dag == dag,
            "cache_key": fncache.to,
            "current_dag": fncache().dag.to,
        }
    return filter_edges(
        {
            "id": ref,
            "cache": cache,
            "argv": dag.argv.to if hasattr(dag, "argv") else None,
            "logs": get_logs(dag),
            "nodes": [make_node(dag.nameof(x), x) for x in dag.nodes],
            "edges": flatten([make_edges(x) for x in dag.nodes]),
            "result": dag.result.to if dag.result is not None else None,
            "error": None if dag.error is None else str(dag.error),
        }
    )
