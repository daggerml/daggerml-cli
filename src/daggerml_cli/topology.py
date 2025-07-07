from daggerml_cli.repo import Fn, Import
from daggerml_cli.util import flatten


def node_info(ref, *, include_argv=True):
    node = ref()
    datume = node.value
    val = node.error if datume is None else datume().value
    data_type = type(val)
    info = {
        "id": ref,
        "doc": node.doc,
        "node_type": type(node.data).__name__.lower(),
        "data_type": data_type.__name__.lower(),
        "length": len(val) if isinstance(val, (list, dict, set)) else None,
        "keys": list(val.keys()) if isinstance(val, dict) else None,
        "datum_id": datume or None,
    }
    if include_argv and isinstance(node.data, Fn):
        info["argv"] = [node_info(x, include_argv=False) for x in node.data.argv]
    elif include_argv:
        info["argv"] = None
    return info


def make_node(name, ref):
    return {"name": name, **node_info(ref)}


def make_edges(ref):
    node = ref()
    out = []
    if isinstance(node.data, Import):
        out.append({"source": node.data.dag, "target": ref, "type": "dag"})
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
