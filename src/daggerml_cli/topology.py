from daggerml_cli.repo import Fn, Import, Resource
from daggerml_cli.util import flatten, tree_map


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
    # print(f"make_edges: {ref} -> {[x['type'] for x in out]}")
    return out


def get_logs(dag):
    logs = getattr(dag, "logs", None)
    if logs is None:
        return
    from daggerml_cli.repo import unroll_datum

    logs = tree_map(lambda x: isinstance(x, Resource), lambda x: x.uri, unroll_datum(logs))
    return logs


def topology(db, ref, cache_db=None):
    dag = ref()
    cache = None
    edges = flatten([make_edges(x) for x in dag.nodes])
    # print(f"topology edges: {[x['type'] for x in edges]}")
    return {
        "id": ref,
        "cache": cache,
        "argv": dag.argv.to if hasattr(dag, "argv") else None,
        "logs": get_logs(dag),
        "nodes": [make_node(dag.nameof(x), x) for x in dag.nodes],
        # "edges": flatten([make_edges(x) for x in dag.nodes]),
        "edges": edges,
        "result": dag.result.to if dag.result is not None else None,
        "error": None if dag.error is None else str(dag.error),
    }
