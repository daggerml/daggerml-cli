from daggerml_cli.repo import Repo, Ref, Resource
from dataclasses import dataclass


@dataclass
class Ctx:
    token: str
    path: str
    head: Ref


def invoke(ctx, data):
    op, arg = data

    if op == 'begin':
        db = Repo(ctx.path)
        with db.tx(True):
            db.checkout(ctx.head)
            db.begin(arg)
            return {'token': db.state}

    if op == 'put_datum':
        if arg['type'] in ['map', 'list', 'scalar']:
            value = arg['value']
        elif arg['type'] == 'set':
            value = set(arg['value'])
        elif arg['type'] == 'resource':
            value = Resource(arg['value'])
        else:
            raise ValueError(f'unknown datum type: {arg["type"]}')
        db = Repo.new(ctx.token)
        with db.tx(True):
            db.checkout(ctx.head)
            ref = db.put_datum(value)
            return {'token': db.state, 'ref': ref.to}

    if op == 'put_node':
        db = Repo.new(ctx.token)
        with db.tx(True):
            db.checkout(ctx.head)
            ref = db.put_node(arg['type'], arg['expr'], Ref(arg['datum']))
            return {'token': db.state, 'ref': ref.to}

    if op == 'commit':
        res_or_err = Ref(arg['ref']) if arg['ref'] else arg['error']
        db = Repo.new(ctx.token)
        with db.tx(True):
            db.checkout(ctx.head)
            ref = db.commit(res_or_err)
            return {'token': None}

    raise ValueError(f'no such op: {op}')
