import json
import logging
import os
import sys
from functools import wraps
from pathlib import Path

import click
import jmespath
from click import ClickException
from tabulate import tabulate

from daggerml_cli import __version__, api
from daggerml_cli.config import Config
from daggerml_cli.repo import REPO_TYPES, Error, Ref, from_json, to_json
from daggerml_cli.util import merge_counters, writefile

logger = logging.getLogger(__name__)

SYSTEM_CONFIG_DIR = str(Path(os.getenv("XDG_CONFIG_HOME", str(Path.home() / ".config"))))
CONFIG_DIR = str((Path(SYSTEM_CONFIG_DIR) / "dml").absolute())
CONFIG_FILE = str((Path(CONFIG_DIR) / "config.yml").absolute())

DEFAULT_CONFIG = {
    "CONFIG_DIR": CONFIG_DIR,
    "PROJECT_DIR": ".dml",
    "REPO": None,
    "BRANCH": None,
    "USER": None,
    "QUERY": None,
    "CACHE_PATH": f"{CONFIG_DIR}/cachedb",
}

BASE_CONFIG = Config(
    os.getenv("DML_CONFIG_DIR", DEFAULT_CONFIG["CONFIG_DIR"]),
    os.getenv("DML_PROJECT_DIR", DEFAULT_CONFIG["PROJECT_DIR"]),
    os.getenv("DML_REPO", DEFAULT_CONFIG["REPO"]),
    os.getenv("DML_BRANCH", DEFAULT_CONFIG["BRANCH"]),
    os.getenv("DML_USER", DEFAULT_CONFIG["USER"]),
    _CACHE_PATH=os.getenv("DML_CACHE_PATH", DEFAULT_CONFIG["CACHE_PATH"]),
)


def jsdumps(x, config=None, full_id=True, **kw):
    result = api.jsdata(x, full_id=full_id)
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
            return sorted([k for k in (api.jsdata(f(ctx.obj or BASE_CONFIG)) or []) if k.startswith(incomplete)])
        except BaseException:
            return []

    return inner


def json_spec(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(json.dumps(ctx.find_root().command.to_info_dict(ctx)))
    ctx.exit()


@click.version_option(version=__version__, prog_name="dml")
@click.option(
    "--user",
    type=str,
    default=DEFAULT_CONFIG["USER"],
    help="Specify user name@host or email, etc.",
)
@click.option(
    "--repo",
    type=str,
    shell_complete=complete(api.with_query(api.list_repo, "[*].name"), set_config),
    help="Specify a repo other than the project repo.",
)
@click.option(
    "--cache-path",
    type=str,
    default=DEFAULT_CONFIG["CACHE_PATH"],
    help="Specify a repo to use as the main cache (full path).",
)
@click.option("--query", type=str, help="A JMESPath query to use in filtering the response data.")
@click.option(
    "--project-dir",
    type=click.Path(),
    default=DEFAULT_CONFIG["PROJECT_DIR"],
    help="Project directory location.",
)
@click.option("--debug", is_flag=True, help="Enable debug output.")
@click.option(
    "--config-dir",
    type=click.Path(),
    default=DEFAULT_CONFIG["CONFIG_DIR"],
    help="Config directory location.",
)
@click.option(
    "--branch",
    type=str,
    shell_complete=complete(api.list_branch, set_config),
    help="Specify a branch other than the project branch.",
)
@click.option(
    "--spec",
    help="Print command info as JSON and exit.",
    is_flag=True,
    expose_value=False,
    callback=json_spec,
    is_eager=True,
)
@click.group(
    no_args_is_help=True,
    context_settings={
        "auto_envvar_prefix": "DML",
        "help_option_names": ["-h", "--help"],
        "show_default": True,
    },
)
@clickex
def cli(ctx, config_dir, project_dir, repo, cache_path, branch, user, query, debug):
    """The DaggerML command line tool."""
    set_config(ctx)
    ctx.with_resource(ctx.obj)


###############################################################################
# API #########################################################################
###############################################################################


@cli.group(name="api", no_args_is_help=True)
@clickex
def api_group(_):
    """DAG builder API commands.
    These commands are normally called only by language client libraries (eg.
    the 'daggerml' python library)."""


@click.argument("message", default="", required=False)
@click.argument("name")
@click.option("--dump", help="Import DAG from a dump.", type=click.File("r"))
@api_group.command(name="create")
@clickex
def api_create(ctx, name, message, dump):
    """Create a new DAG.
    A new DAG named NAME is created with a descriptive commit MESSAGE. A token
    is printed to stdout which can be used to invoke DAG builder API methods
    on this DAG. If the --dump option is provided a function DAG is created."""
    try:
        dump = dump if dump is None else dump.read()
        idx = api.begin_dag(ctx.obj, name=name, message=message, dump=dump)
        click.echo(to_json(idx))
    except Exception as e:
        click.echo(to_json(Error(e)))


@click.argument("data", type=click.File("r"), default="-", required=False)
@click.argument("token")
@api_group.command(name="invoke")
@clickex
def api_invoke(ctx, token, data):
    """Invoke DAG builder API methods.
    API methods are invoked with the TOKEN returned by the 'dag create' command
    and JSON consisting of a serialized payload of the form:

        [method, [args...] {kwargs...}]"""
    try:
        click.echo(to_json(api.invoke_api(ctx.obj, from_json(token), from_json(data.read().strip()))))
    except Exception as e:
        click.echo(to_json(Error(e)))


###############################################################################
# BRANCH ######################################################################
###############################################################################


@cli.group(name="branch", no_args_is_help=True)
@clickex
def branch_group(ctx):
    """Branch management commands."""


@click.argument(
    "commit",
    required=False,
    shell_complete=complete(api.with_query(api.list_commit, "[*].id")),
)
@click.argument("name")
@branch_group.command(name="create")
@clickex
def branch_create(ctx, name, commit):
    """Create a new branch."""
    api.create_branch(ctx.obj, name, commit)
    click.echo(f"Created branch: {name}")


@click.argument("name", shell_complete=complete(api.list_other_branch))
@branch_group.command(name="delete")
@clickex
def branch_delete(ctx, name):
    """Delete a branch."""
    api.delete_branch(ctx.obj, name)
    click.echo(f"Deleted branch: {name}")


@branch_group.command(name="list")
@clickex
def branch_list(ctx):
    """List branches."""
    click.echo(jsdumps(api.list_branch(ctx.obj), ctx.obj, full_id=False))


@click.argument("branch", shell_complete=complete(api.list_other_branch))
@branch_group.command(name="merge")
@clickex
def branch_merge(ctx, branch):
    """Merge another branch with the current one."""
    click.echo(api.merge_branch(ctx.obj, branch))


@click.argument("branch", shell_complete=complete(api.list_other_branch))
@branch_group.command(name="rebase")
@clickex
def branch_rebase(ctx, branch):
    """Rebase the current branch onto another one."""
    click.echo(api.rebase_branch(ctx.obj, branch))


###############################################################################
# COMMIT ######################################################################
###############################################################################


@cli.group(name="commit", no_args_is_help=True)
@clickex
def commit_group(_):
    """Commit management commands."""


@commit_group.command(name="list")
@clickex
def commit_list(ctx):
    """List commits."""
    click.echo(jsdumps(api.list_commit(ctx.obj), ctx.obj))


@commit_group.command(name="describe")
@click.argument("commit", required=False, shell_complete=complete(api.with_query(api.list_commit, "[*].id")))
@clickex
def commit_describe(ctx, commit=None):
    """List commits."""
    commit = Ref(commit) if commit else None
    click.echo(jsdumps(api.describe_commit(ctx.obj, commit), ctx.obj))


@click.option("--output", type=click.Choice(["json", "ascii"]), default="ascii", help="Print a graph of all commits.")
@commit_group.command(name="log")
@clickex
def commit_log(ctx, output):
    """Query the commit log."""
    resp = api.commit_log_graph(ctx.obj, output=output)
    if output == "json":
        click.echo(jsdumps(resp, ctx.obj))


@click.argument("commit", shell_complete=complete(api.with_query(api.list_commit, "[*].id")))
@commit_group.command(name="revert")
@clickex
def commit_revert(ctx, commit):
    """Revert a commit."""
    return api.revert_commit(ctx.obj, commit)


###############################################################################
# CONFIG ######################################################################
###############################################################################


@cli.group(name="config", no_args_is_help=True)
@clickex
def config_group(_):
    """Configuration settings."""


@click.argument("repo", shell_complete=complete(api.with_query(api.list_other_repo, "[*].name")))
@config_group.command(name="repo")
@clickex
def config_repo(ctx, repo):
    """Select the repository to use."""
    api.config_repo(ctx.obj, repo)
    click.echo(f"Selected repository: {repo}")


@click.argument("name", shell_complete=complete(api.list_other_branch))
@config_group.command(name="branch")
@clickex
def config_branch(ctx, name):
    """Select the branch to use."""
    api.config_branch(ctx.obj, name)
    click.echo(f"Selected branch: {name}")


@click.argument("user", shell_complete=complete(api.list_other_branch))
@config_group.command(name="user")
@clickex
def config_user(ctx, user):
    """Set user name/email/etc."""
    api.config_user(ctx.obj, user)
    click.echo(f"Set user: {user}")


###############################################################################
# DAG #########################################################################
###############################################################################


@cli.group(name="dag", no_args_is_help=True)
@clickex
def dag_group(_):
    """DAG management commands."""


@dag_group.command(name="list")
@click.option("--all", is_flag=True, help="List all dags or only named dags?")
@clickex
def dag_list(ctx, all):
    """List DAGs."""
    click.echo(jsdumps(api.list_dags(ctx.obj, all=all), ctx.obj))


@click.argument("message", type=str)
@click.argument("name", type=str, shell_complete=complete(api.with_query(api.list_dags, "[*].name")))
@dag_group.command(name="delete")
@clickex
def dag_delete(ctx, name, message):
    """Delete a DAG."""
    ref = ([x.id for x in api.list_dags(ctx.obj) if x.name == name] or [None])[0]
    assert ref, f"no such dag: {name}"
    api.delete_dag(ctx.obj, name, message)
    click.echo(f"Deleted dag: {name}")


@click.argument("name", type=str, shell_complete=complete(api.with_query(api.list_dags, "[*].name")))
@dag_group.command(name="describe")
@clickex
def dag_describe(ctx, name):
    """Describe a DAG"""
    ref = api.get_dag(ctx.obj, name)
    if ref is None:
        click.echo("no such dag", err=True)
        raise ValueError(f"no such dag fool: {name}")
    graph = api.describe_dag(ctx.obj, ref)
    click.echo(jsdumps(graph))


###############################################################################
# NODE ########################################################################
###############################################################################


@cli.group(name="node", no_args_is_help=True)
@clickex
def node_group(_):
    """Node management commands."""


@node_group.command(name="backtrack")
@click.argument("node_id", type=str)
@click.argument("keys", type=str, nargs=-1)
@clickex
def node_backtrack(ctx, node_id, keys):
    """Backtrack a node within a dag.

    If you insert a literal node with another node inside somewhere, this is how
    you can get the original back. This is useful when the original node was in
    import or a function node, and you want to get to that node's dag.

    For example, let's say you're doing a parameter search. For each parameter
    set, you run the function with those parameters and then save the results
    as a list of dicts: `[{"params": params, "result": fn(params)}, ...]`, where
    `fn` is a function node. If you want to get some intermediate results out of
    `results_node[0]["result"]`, you can use this command to backtrack like so:
    `dml node backtrack <result-node-id> 0 result`.

    If you instead use the get methods, you would get new nodes unassociated
    with the original function node, which is not what you want.

    \b
    Notes:
    - This command does not add nodes to the DAG.
    - This command only works with collection nodes created via put_literal.
    """
    click.echo(to_json(api.backtrack_node(ctx.obj, Ref(node_id), *keys)))


@node_group.command(name="describe")
@click.argument("node_id", type=str)
@clickex
def node_describe(ctx, node_id):
    """Get information about a node."""
    click.echo(jsdumps(api.describe_node(ctx.obj, Ref(node_id))))


###############################################################################
# CACHE #######################################################################
###############################################################################


@cli.group(name="cache", no_args_is_help=True)
@clickex
def cache_group(_):
    """cache management commands."""


@cache_group.command(name="create")
@clickex
def cache_create(ctx):
    """Create a fndag cache."""
    click.echo(jsdumps(api.create_cache(ctx.obj)))


@cache_group.command(name="list")
@clickex
def cache_list(ctx):
    """List cached dags."""
    click.echo(jsdumps(api.list_cache(ctx.obj)))


@cache_group.command(name="info")
@click.argument("cache_key", type=str)
@clickex
def cache_info(ctx, cache_key):
    """Prints information about what's in the cache for a given key."""
    click.echo(jsdumps(api.info_cache(cache_key)))


@cache_group.command(name="delete")
@click.argument("cache_key", type=str)
@clickex
def cache_delete(ctx, cache_key):
    """Delete a cached item."""
    api.delete_cache(ctx.obj, cache_key)
    click.echo(f"Deleted: {cache_key!r} from cache")


@cache_group.command(name="put")
@click.argument("dag_id", type=str)
@clickex
def cache_put(ctx, dag_id):
    """Adds an item to the cache."""
    click.echo(jsdumps({"cache_key": api.put_cache(ctx.obj, Ref(dag_id))}))


###############################################################################
# INDEX #######################################################################
###############################################################################


@cli.group(name="index", no_args_is_help=True)
@clickex
def index_group(_):
    """Index management commands."""


@index_group.command(name="list")
@clickex
def index_list(ctx):
    """List indexes."""
    click.echo(jsdumps(api.list_indexes(ctx.obj), ctx.obj))


@click.argument("id", shell_complete=complete(api.with_query(api.list_indexes, "[*].id")))
@index_group.command(name="delete")
@clickex
def index_delete(ctx, id):
    """Delete index."""
    if api.delete_index(ctx.obj, Ref(f"index/{id}")):
        click.echo(f"Deleted index: {id}")


###############################################################################
# REF #########################################################################
###############################################################################


@cli.group(name="ref", no_args_is_help=True)
@clickex
def ref_group(_):
    """Ref management commands."""


@click.argument("id", type=str)
@click.argument("type", type=click.Choice(sorted(REPO_TYPES)))
@ref_group.command(name="describe")
@clickex
def ref_describe(ctx, type, id):
    """Get the properties of a ref as JSON."""
    click.echo(jsdumps(from_json(api.dump_ref(ctx.obj, Ref(f"{type}/{id}"), recursive=False))[0][1]))


@click.argument("ref", type=str)
@ref_group.command(name="dump")
@clickex
def ref_dump(ctx, ref):
    """Dump a ref and all its dependencies to JSON."""
    dump = api.dump_ref(ctx.obj, from_json(ref))
    click.echo(dump)


@ref_group.command(name="load")
@click.argument("json", type=str)
@clickex
def ref_load(ctx, json):
    """Load a previously dumped ref into the repo."""
    ref = api.load_ref(ctx.obj, json)
    click.echo(to_json(ref))


###############################################################################
# REMOTE ######################################################################
###############################################################################


@cli.group(name="remote", no_args_is_help=True)
@clickex
def remote_group(ctx):
    """Repository tracking commands."""


###############################################################################
# REPO ########################################################################
###############################################################################


@cli.group(name="repo", no_args_is_help=True)
@clickex
def repo_group(ctx):
    """Repository management commands."""


@click.argument("name")
@repo_group.command(name="create")
@clickex
def repo_create(ctx, name):
    """Create a new repository."""
    api.create_repo(ctx.obj, name)
    click.echo(f"Created repository: {name}")


@repo_group.command(name="resize")
@click.argument("name")
@click.argument("size", type=int)
@clickex
def repo_resize(ctx, name, size):
    """Resize a repository."""
    api.resize_repo(ctx.obj, name, size)
    click.echo(f"Resized repository: {name} to {size / 1024 / 1024} MB")


@click.argument("name", shell_complete=complete(api.with_query(api.list_repo, "[*].name")))
@repo_group.command(name="delete")
@clickex
def repo_delete(ctx, name):
    """Delete a repository."""
    api.delete_repo(ctx.obj, name)
    click.echo(f"Deleted repository: {name}")


@click.argument("name")
@repo_group.command(name="copy")
@clickex
def repo_copy(ctx, name):
    """Copy this repository to NAME."""
    api.copy_repo(ctx.obj, name)
    click.echo(f"Copied repository: {ctx.obj.REPO} -> {name}")


@repo_group.command(name="list")
@clickex
def repo_list(ctx):
    """List repositories."""
    click.echo(jsdumps(api.list_repo(ctx.obj), ctx.obj))


@click.option(
    "--remove",
    help="Remove the deleted item with the given ID.",
    type=str,
    shell_complete=complete(api.with_query(api.list_deleted, "[*].id")),
)
@repo_group.command(name="deleted")
@clickex
def repo_deleted(ctx, remove=None):
    """List or remove deleted resources."""
    if remove:
        api.remove_deleted(ctx.obj, Ref(f"deleted/{remove}"))
        click.echo(f"Removed deleted: {remove}")
    else:
        click.echo(jsdumps(api.list_deleted(ctx.obj), ctx.obj))


@repo_group.command(name="gc")
@clickex
def repo_gc(ctx):
    """Delete unreachable objects.
    A summary table of objects deleted by type is printed. Resource objects
    which were deleted can be accessed via `dml repo deleted` so that their
    associated external resources can be cleaned up."""
    deleted, remaining = api.gc_repo(ctx.obj)
    summary = [[k, *v] for k, v in merge_counters(deleted, remaining).items()]
    summary = sorted(summary, key=lambda x: x[0])
    headers = ["object", "deleted", "remaining"]
    click.echo(tabulate(summary, headers=headers, tablefmt="plain"))


###############################################################################
# STATUS ######################################################################
###############################################################################


@cli.command(name="status")
@clickex
def cli_status(ctx):
    """Show the current repo, branch, etc."""
    click.echo(jsdumps(api.status(ctx.obj), ctx.obj))


###############################################################################
# UTIL ########################################################################
###############################################################################


@cli.group(name="util", no_args_is_help=True)
@clickex
def util_group(_):
    """Various utility commands."""


@click.argument("file", type=click.Path())
@click.argument("dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option(
    "--exclude",
    help="A glob style wildcard pattern of files to exclude.",
    type=str,
    multiple=True,
)
@util_group.command(name="tar")
@clickex
def util_tar(ctx, dir, file, exclude):
    """Create a reproducible archive.
    Given a directory DIR and an output FILE, a tarball is created such that the
    hash of the tarball depends only on the contents of the files and directories
    in DIR, ie. the tarball has the same hash regardless of where or when it was
    created, who created it, or which operating system it was created on.

    The --exclude option can be specified multiple times; it will be passed as
    command line options to GNU tar.

    Note: You must have GNU tar installed as 'tar' or 'gtar' on your PATH.
    """
    api.reproducible_tar(dir, file, *exclude)
