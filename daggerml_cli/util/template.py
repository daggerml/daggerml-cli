from daggerml_cli.util.restapi import api

def parse(template_file, args):
    with open(template_file, 'r') as f:
        return api({
            'op': 'eval_dag',
            'body': f.read(),
            'args': '{}',
        })
