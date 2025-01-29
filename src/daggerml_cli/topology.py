from daggerml_cli.repo import Datum, Fn, Import, Literal, Ref, unroll_datum
from daggerml_cli.util import assoc, flatten


def node_value(x):
    if isinstance(x, Ref):
        return node_value(x())
    if isinstance(x, Datum):
        return unroll_datum(x)
    if isinstance(x, (list, tuple)):
        return f'{[node_value(y) for y in x]}'
    if isinstance(x, Fn):
        return node_value(x.dag().result().data)
    if isinstance(x, Literal):
        return f'{node_value(x.value().value)}'
    if isinstance(x, Import):
        return f'{x.dag().result().value().value}'
    return x


def make_node(name, ref):
    node = ref()
    return {
        'id': ref,
        'name': name,
        'doc': node.doc,
        'type': type(node.data).__name__.lower(),
        'value': node_value(node.data),
    }


def make_edges(ref):
    node = ref()
    if isinstance(node.data, Fn):
        return [{'source': x, 'target': ref, 'type': 'node'} for x in set(node.data.argv)]
    if isinstance(node.data, Import):
        return [{'source': ref, 'target': node.data.dag, 'type': 'dag'}]
    return []


def filter_edges(topology):
    def valid(x):
        return x['type'] == 'dag' or {x['source'], x['target']} < nodes
    nodes = {x['id'] for x in topology['nodes']}
    return assoc(topology, 'edges', list(filter(valid, topology['edges'])))


def topology(ref):
    dag = ref()
    return filter_edges({
        'id': ref,
        'argv': dag.argv.id if hasattr(dag, 'argv') else None,
        'nodes': [make_node(dag.nameof(x), x) for x in dag.nodes],
        'edges': flatten([make_edges(x) for x in dag.nodes]),
        'result': dag.result.id if dag.result is not None else None,
        'error': None if dag.error is None else str(dag.error),
    })
