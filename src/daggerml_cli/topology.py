from daggerml_cli.repo import Fn, Import
from daggerml_cli.util import flatten


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


def topology(db, ref):
    dag = ref()
    edges = flatten([make_edges(x) for x in dag.nodes])
    return {
        "id": ref,
        "argv": dag.argv.to if hasattr(dag, "argv") else None,
        "cache_key": dag.argv().value.id if hasattr(dag, "argv") else None,
        "nodes": [make_node(dag.nameof(x), x) for x in dag.nodes],
        "edges": edges,
        "result": dag.result.to if dag.result is not None else None,
        "error": None if dag.error is None else str(dag.error),
    }
