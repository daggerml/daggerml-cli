import json
import os
from functools import wraps
from pathlib import Path

import click
import jmespath
from click import ClickException

from daggerml_cli import api
from daggerml_cli.__about__ import __version__
from daggerml_cli.config import Config
from daggerml_cli.db import DB_TYPES
from daggerml_cli.repo import Error, Ref, from_json, to_json

SYSTEM_CONFIG_DIR = str(Path(os.getenv('XDG_CONFIG_HOME', str(Path.home() / '.config'))))
CONFIG_DIR = str((Path(SYSTEM_CONFIG_DIR) / 'dml').absolute())
CONFIG_FILE = str((Path(CONFIG_DIR) / 'config.yml').absolute())

DEFAULT_CONFIG = {
    'CONFIG_DIR': CONFIG_DIR,
    'PROJECT_DIR': '.dml',
    'REPO': None,
    'BRANCH': None,
    'USER': None,
    'QUERY': None,
}

BASE_CONFIG = Config(
    os.getenv('DML_CONFIG_DIR', DEFAULT_CONFIG['CONFIG_DIR']),
    os.getenv('DML_PROJECT_DIR', DEFAULT_CONFIG['PROJECT_DIR']),
    os.getenv('DML_REPO', DEFAULT_CONFIG['REPO']),
    os.getenv('DML_BRANCH', DEFAULT_CONFIG['BRANCH']),
    os.getenv('DML_USER', DEFAULT_CONFIG['USER']),
)


def jsdumps(x, config=None, **kw):
    result = api.jsdata(x)
    if config is not None and config.QUERY is not None:
        result = jmespath.search(config.QUERY, result)
    return json.dumps(result, indent=2, **kw)


def set_config(ctx, *_):
    ctx.obj = Config.new(**dict(ctx.params.items()))


def clickex(f):
    @wraps(f)
    def inner(ctx, *args, **kwargs):
        try:
            return f(ctx, *args, **kwargs)
        except BaseException as e:
            raise (e if ctx.obj.DEBUG else ClickException(str(e))) from e
    return click.pass_context(inner)


def complete(f, prelude=None):
    def inner(ctx, param, incomplete):
        try:
            if prelude:
                prelude(ctx, param, incomplete)
            return [k for k in (api.jsdata(f(ctx.obj or BASE_CONFIG)) or []) if k.startswith(incomplete)]
        except BaseException:
            return []
    return inner


def json_spec(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(json.dumps(ctx.find_root().command.to_info_dict(ctx)))
    ctx.exit()


@click.version_option(version=__version__, prog_name='dml')
@click.option(
    '--user',
    type=str,
    default=DEFAULT_CONFIG['USER'],
    help='Specify user name@host or email, etc.')
@click.option(
    '--repo',
    type=str,
    shell_complete=complete(api.with_query(api.list_repo, '[*].name'), set_config),
    help='Specify a repo other than the project repo.')
@click.option(
    '--query',
    type=str,
    help='A JMESPath query to use in filtering the response data.')
@click.option(
    '--project-dir',
    type=click.Path(),
    default=DEFAULT_CONFIG['PROJECT_DIR'],
    help='Project directory location.')
@click.option(
    '--debug',
    is_flag=True,
    help='Enable debug output.')
@click.option(
    '--config-dir',
    type=click.Path(),
    default=DEFAULT_CONFIG['CONFIG_DIR'],
    help='Config directory location.')
@click.option(
    '--branch',
    type=str,
    shell_complete=complete(api.list_branch, set_config),
    help='Specify a branch other than the project branch.')
@click.option(
    '--spec',
    help='Print command info as JSON and exit.',
    is_flag=True,
    expose_value=False,
    callback=json_spec,
    is_eager=True,
)
@click.group(
    no_args_is_help=True,
    context_settings={
        "auto_envvar_prefix": "DML",
        'help_option_names': ['-h', '--help'],
        'show_default': True,
    })
@clickex
def cli(ctx, config_dir, project_dir, repo, branch, user, query, debug):
    """The DaggerML command line tool."""
    set_config(ctx)
    ctx.with_resource(ctx.obj)


###############################################################################
# STATUS ######################################################################
###############################################################################


@cli.command(name='status', help='Current repo, branch, etc.')
@clickex
def cli_status(ctx):
    click.echo(jsdumps(api.status(ctx.obj), ctx.obj))


###############################################################################
# REF #########################################################################
###############################################################################


@cli.group(name='ref', no_args_is_help=True, help='Ref management commands.')
@clickex
def ref_group(_):
    pass


@click.argument('id', type=str)
@click.argument('type', type=click.Choice(DB_TYPES))
@ref_group.command(name='describe', help='Get the properties of a ref as JSON.')
@clickex
def ref_describe(ctx, type, id):
    click.echo(jsdumps(from_json(api.dump_ref(ctx.obj, Ref(f'{type}/{id}'), False))[0][1]))


@click.argument('ref', type=str)
@ref_group.command(name='dump', help='Dump a ref and all its dependencies to JSON.')
@clickex
def ref_dump(ctx, ref):
    dump = api.dump_ref(ctx.obj, from_json(ref))
    click.echo(to_json(dump))


@ref_group.command(name='load', help='Load a previously dumped ref into the repo.')
@click.argument('json', type=str)
@clickex
def ref_load(ctx, json):
    ref = api.load_ref(ctx.obj, from_json(json))
    click.echo(to_json(ref))


###############################################################################
# REPO ########################################################################
###############################################################################


@cli.group(name='repo', no_args_is_help=True, help='Repository management commands.')
@clickex
def repo_group(ctx):
    pass


@click.argument('name')
@repo_group.command(name='create', help='Create a new repository.')
@clickex
def repo_create(ctx, name):
    api.create_repo(ctx.obj, name)
    click.echo(f'Created repository: {name}')


@click.argument('name', shell_complete=complete(api.with_query(api.list_repo, '[*].name')))
@repo_group.command(name='delete', help='Delete a repository.')
@clickex
def repo_delete(ctx, name):
    api.delete_repo(ctx.obj, name)
    click.echo(f'Deleted repository: {name}')


@click.argument('name')
@repo_group.command(name='copy', help='Copy this repository to NAME.')
@clickex
def repo_copy(ctx, name):
    api.copy_repo(ctx.obj, name)
    click.echo(f'Copied repository: {ctx.obj.REPO} -> {name}')


@repo_group.command(name='list', help='List repositories.')
@clickex
def repo_list(ctx):
    click.echo(jsdumps(api.list_repo(ctx.obj), ctx.obj))


@repo_group.command(name='gc', help='Delete unreachable objects in the repo.')
@clickex
def repo_gc(ctx):
    for rsrc in api.gc_repo(ctx.obj):
        click.echo(rsrc.uri)


###############################################################################
# CONFIG ######################################################################
###############################################################################


@cli.group(name='config', no_args_is_help=True, help='Configuration settings.')
@clickex
def config_group(_):
    pass


@click.argument('repo', shell_complete=complete(api.with_query(api.list_repo, '[*].name')))
@config_group.command(name='repo', help='Select the repository to use.')
@clickex
def config_repo(ctx, repo):
    api.config_repo(ctx.obj, repo)
    click.echo(f'Selected repository: {repo}')


@click.argument('name', shell_complete=complete(api.list_other_branch))
@config_group.command(name='branch', help='Select the branch to use.')
@clickex
def config_branch(ctx, name):
    api.config_branch(ctx.obj, name)
    click.echo(f'Selected branch: {name}')


@click.argument('user', shell_complete=complete(api.list_other_branch))
@config_group.command(name='user', help='Set user name/email/etc.')
@clickex
def config_user(ctx, user):
    api.config_user(ctx.obj, user)
    click.echo(f'Set user: {user}')


###############################################################################
# BRANCH ######################################################################
###############################################################################


@cli.group(name='branch', no_args_is_help=True, help='Branch management commands.')
@clickex
def branch_group(ctx):
    pass


@click.argument('commit', required=False, shell_complete=complete(api.with_query(api.list_commit, '[*].id')))
@click.argument('name')
@branch_group.command(name='create', help='Create a new branch.')
@clickex
def branch_create(ctx, name, commit):
    api.create_branch(ctx.obj, name, commit)
    click.echo(f'Created branch: {name}')


@click.argument('name', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='delete', help='Delete a branch.')
@clickex
def branch_delete(ctx, name):
    api.delete_branch(ctx.obj, name)
    click.echo(f'Deleted branch: {name}')


@branch_group.command(name='list', help='List branches.')
@clickex
def branch_list(ctx):
    click.echo(jsdumps(api.list_branch(ctx.obj), ctx.obj))


@click.argument('branch', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='merge', help='Merge another branch with the current one.')
@clickex
def branch_merge(ctx, branch):
    click.echo(api.merge_branch(ctx.obj, branch))


@click.argument('branch', shell_complete=complete(api.list_other_branch))
@branch_group.command(name='rebase', help='Rebase the current branch onto another one.')
@clickex
def branch_rebase(ctx, branch):
    click.echo(api.rebase_branch(ctx.obj, branch))


###############################################################################
# DAG #########################################################################
###############################################################################


@cli.group(name='dag', no_args_is_help=True, help='DAG management commands.')
@clickex
def dag_group(_):
    pass


@dag_group.command(name='list', help='List DAGs.')
@clickex
def dag_list(ctx):
    click.echo(jsdumps(api.list_dags(ctx.obj), ctx.obj))


@click.argument('name', type=str, shell_complete=complete(api.with_query(api.list_dags, '[*].name')))
@dag_group.command(name='describe', help='Get the properties of a dag as JSON.')
@clickex
def dag_describe(ctx, name):
    ref = ([x.id for x in api.list_dags(ctx.obj) if x.name == name] or [None])[0]
    assert ref, f'no such dag: {name}'
    click.echo(jsdumps(api.describe_dag(ctx.obj, ref)))


@click.argument('name', type=str, shell_complete=complete(api.with_query(api.list_dags, '[*].name')))
@dag_group.command(name='html', help='Get the dag html page printed to stdout.')
@clickex
def dag_html(ctx, name):
    ref = ([x.id for x in api.list_dags(ctx.obj) if x.name == name] or [None])[0]
    assert ref, f'no such dag: {name}'
    click.echo(api.write_dag_html(ctx.obj, ref))


###############################################################################
# API #########################################################################
###############################################################################


@cli.group(name='api', no_args_is_help=True, help='DAG builder API commands.')
@clickex
def api_group(_):
    pass


@click.argument('message')
@click.argument('name')
@click.option('--dump', help='Import DAG from a dump.', type=str)
@api_group.command(name='create', help='Create a new DAG.')
@clickex
def api_create(ctx, name, message, dump=None):
    try:
        idx = api.begin_dag(ctx.obj, name=name, message=message, dump=dump)
        click.echo(to_json(idx))
    except Exception as e:
        click.echo(to_json(Error(e)))


@click.argument('json')
@click.argument('token')
@api_group.command(
    name='invoke',
    help=f'Invoke API with token returned by create and JSON command body.\n\nJSON command ops: {api.format_ops()}')
@clickex
def api_invoke(ctx, token, json):
    try:
        click.echo(to_json(api.invoke_api(
            ctx.obj, from_json(token), from_json(json))))
    except Exception as e:
        click.echo(to_json(Error(e)))


###############################################################################
# INDEX #######################################################################
###############################################################################


@cli.group(name='index', no_args_is_help=True, help='Index management commands.')
@clickex
def index_group(_):
    pass


@index_group.command(name='list', help="List indexes.")
@clickex
def index_list(ctx):
    click.echo(jsdumps(api.list_indexes(ctx.obj), ctx.obj))


@click.argument('id', shell_complete=complete(api.with_query(api.list_indexes, '[*].id')))
@index_group.command(name='delete', help="Delete index.")
@clickex
def index_delete(ctx, id):
    if api.delete_index(ctx.obj, Ref(f'index/{id}')):
        click.echo(f'Deleted index: {id}')


###############################################################################
# COMMIT ######################################################################
###############################################################################


@cli.group(name='commit', no_args_is_help=True, help='Commit management commands.')
@clickex
def commit_group(_):
    pass


@commit_group.command(name='list', help='List commits.')
@clickex
def commit_list(ctx):
    click.echo(jsdumps(api.list_commit(ctx.obj), ctx.obj))


@click.option('--graph', is_flag=True, help='Print a graph of all commits.')
@commit_group.command(name='log', help='Query the commit log.')
@clickex
def commit_log(ctx, graph):
    return api.commit_log_graph(ctx.obj)


@click.argument('commit', shell_complete=complete(api.with_query(api.list_commit, '[*].id')))
@commit_group.command(name='revert', help='Revert a commit.')
@clickex
def commit_revert(ctx, commit):
    return api.revert_commit(ctx.obj, commit)
