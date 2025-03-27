from daggerml_cli.repo import Fn, Import, Resource
from daggerml_cli.util import assoc, flatten, tree_map


def make_node(name, ref):
    node = ref()
    return {
        "id": ref,
        "name": name,
        "doc": node.doc,
        "node_type": type(node.data).__name__.lower(),
        "data_type": type(node.data.value().value).__name__.lower(),
    }


def make_edges(ref):
    node = ref()
    out = []
    if isinstance(node.data, Import):
        out.append({"source": ref, "target": node.data.dag, "type": "dag"})
    if isinstance(node.data, Fn):
        out.extend([{"source": x, "target": ref, "type": "node", "arg": i} for i, x in enumerate(node.data.argv)])
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


def topology(ref):
    dag = ref()
    return filter_edges(
        {
            "id": ref,
            "argv": dag.argv.id if hasattr(dag, "argv") else None,
            "logs": get_logs(dag),
            "nodes": [make_node(dag.nameof(x), x) for x in dag.nodes],
            "edges": flatten([make_edges(x) for x in dag.nodes]),
            "result": dag.result.id if dag.result is not None else None,
            "error": None if dag.error is None else str(dag.error),
        }
    )
