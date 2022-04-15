import sys
import typer
from daggerml.util import api, print_result


_app = typer.Typer(result_callback=print_result)


@_app.command()
def get_datum_info(name: str):
    """Gets datum info

    Parameters
    ----------
    name : str
        the fully qualified datum name. Can be a ref or whatever.
    """
    return api({'op': 'get_datum_info', 'name': name})


literal_parsers = {
    'string': lambda x: x,
    'int': int,
    'float': float,
    'null': lambda x: None
}

def parse_datum(value):
    if value['type'] == 'literal':
        _type, val = value['data']['type'], value['data']['value']
        return {'type': 'literal', 'value': literal_parsers[_type](val)}
    elif value['type'] == 'blob':
        return {'type': 'blob', 'location': value['data']['location']}
    else:
        raise ValueError('Unrecognized type: %s' % value['type'])


def read_datum(name):
    info = get_datum_info(name)
    return parse_datum(info['result']['value'])


@_app.command('read-datum')
def cli_read_datum(name: str, preview_rows: int = 0):
    datum = read_datum(name)
    if preview_rows > 0:
        try:
            import polars as pl
            pl.Config.set_tbl_rows(preview_rows)
            print(pl.read_csv(datum['location'], n_rows=preview_rows), file=sys.stderr)
        except ImportError:
            pass
    return {'status': 'ok', 'datum': datum}
