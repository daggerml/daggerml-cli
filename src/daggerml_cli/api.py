import json
import logging
import os
import shutil
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass, is_dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import jmespath
from asciidag.graph import Graph as AsciiGraph
from asciidag.node import Node as AsciiNode

from daggerml_cli.repo import (
    BUILTIN_FNS,
    DEFAULT_BRANCH,
    CheckedRef,
    Ctx,
    Dag,
    Error,
    Import,
    Index,
    Literal,
    Node,
    Ref,
    Repo,
    Resource,
    from_json,
    to_json,
    unroll_datum,
)
from daggerml_cli.topology import topology
from daggerml_cli.util import (
    asserting,
    detect_executable,
    makedirs,
    readfile,
    run,
    some,
    writefile,
)

###############################################################################
# HELPERS #####################################################################
###############################################################################

log = logging.getLogger(__name__)


def jsdata(x):
    if isinstance(x, (tuple, list, set)):
        return [jsdata(y) for y in x]
    if isinstance(x, dict):
        return {k: jsdata(v) for k, v in x.items()}
    if isinstance(x, Ref):
        return x.id
    if is_dataclass(x):
        return jsdata(x.__dict__)
    return x


def with_query(f, query):
    return lambda *args, **kwargs: jmespath.search(query, jsdata(f(*args, **kwargs)))


def with_attrs(x, **kwargs):
    y = x()
    for k, v in kwargs.items():
        setattr(y, k, v)
    return y


@contextmanager
def tx(config, write=False):
    with Repo(config.REPO_PATH, head=config.BRANCHREF, user=config.USER) as db:
        with db.tx(write):
            yield db


###############################################################################
# REMOTE ######################################################################
###############################################################################


DEFAULT_TAG = "00000000000000000000000000000000"


def remote_handler_name(uri):
    return f"dml-remote-{urlparse(uri).scheme}-handler"


def remote_handler_path(uri):
    return shutil.which(remote_handler_name(uri))


def remote_base(config):
    return os.path.join(config.CONFIG_DIR, "remote")


def remote_path(config, name):
    return os.path.join(remote_base(config), name)


def ref2remote(ref, name):
    return replace(ref, to=f"{ref.type}/{name}/{ref.id}")


def ref2local(ref, name):
    return replace(ref, to=f"{ref.type}/{ref.id.split('/', 1)[1]}")


def load_fetch(remote_dir, local_dir, name, head=None):
    with Repo(remote_dir) as rem, Repo(local_dir) as loc:
        with rem.tx(), loc.tx(True):
            heads = [head] if head else rem.objects("")
            for dump in [from_json(rem.dump_ref(x)) for x in heads]:
                dump[-1][0] = ref2remote(dump[-1][0], name)
                loc.load_ref(to_json(dump))


def download_remote(config, name, config_dir, repo):
    assert os.path.isdir(remote_path(config, name)), f"no such remote: {name}"
    uri = readfile(remote_path(config, name), "uri")
    handler = remote_handler_path(uri)
    repo_path = makedirs(os.path.join(config_dir, "repo", repo))
    repo_file = os.path.join(repo_path, "data.mdb")
    assert not os.path.exists(repo_file), f"repo already exists: {repo}"
    tag = run([handler, "tag", uri])
    with open(repo_file, "wb") as f:
        f.write(run([handler, "get", uri, tag], text=False))
    return repo_path, tag


def create_remote(config, name, uri):
    handler = remote_handler_path(uri)
    path = remote_path(config, name)
    assert handler, f"protocol handler not found on PATH: {remote_handler_name(uri)}"
    assert not os.path.exists(path), f"remote already exists: {name}"
    writefile(uri, makedirs(path), "uri")


def delete_remote(config, name):
    shutil.rmtree(remote_path(config, name))


def list_remote(config):
    dir = remote_base(config)
    if os.path.isdir(dir):
        xs = sorted(os.listdir(dir))
        uri = lambda x: readfile(remote_path(config, x), "uri")  # noqa: E731
        return [{"name": x, "uri": uri(x), "path": os.path.join(dir, x)} for x in xs]
    return []


def clone_remote(config, name, repo):
    download_remote(config, name, config.CONFIG_DIR, repo)


def fetch_remote(config, name):
    with TemporaryDirectory() as config_dir:
        (repo_path,) = download_remote(config, name, config_dir)
        load_fetch(repo_path, config.REPO_PATH, name, config.REPO)


def pull_remote(config, name):
    fetch_remote(config, name)
    merge_branch(config, f"{name}/{config.BRANCH}")


def push_remote(config, name):
    with TemporaryDirectory() as config_dir:
        (repo_path, tag) = download_remote(config, name, config_dir)
        load_fetch(config.CONFIG_DIR, repo_path, name, config.REPO)
        merge_branch(replace(config, _CONFIG_DIR=config_dir))


###############################################################################
# REPO ########################################################################
###############################################################################


def repo_path(config):
    return config.REPO_PATH


def list_repo(config):
    if os.path.exists(config.REPO_DIR):
        xs = sorted(os.listdir(config.REPO_DIR))
        return [{"name": x, "path": os.path.join(config.REPO_DIR, x)} for x in xs]
    return []


def list_other_repo(config):
    return [k for k in list_repo(config) if k["name"] != config.REPO]


def create_repo(config, name):
    config._REPO = name
    with Repo(makedirs(config.REPO_PATH), config.USER, create=True):
        pass


def delete_repo(config, name):
    path = os.path.join(config.REPO_DIR, name)
    shutil.rmtree(path)


def copy_repo(config, name):
    with Repo(config.REPO_PATH) as db:
        db.copy(os.path.join(config.REPO_DIR, name))


def gc_repo(config):
    with Repo(config.REPO_PATH) as db:
        with db.tx(True):
            return db.gc()


def list_deleted(config):
    with Repo(config.REPO_PATH) as db:
        with db.tx():
            return [{"id": x, **x().__dict__} for x in db.objects("deleted")]


def remove_deleted(config, ref):
    with Repo(config.REPO_PATH) as db:
        with db.tx(True):
            assert ref.type == "deleted"
            db.delete(ref)


###############################################################################
# REF #########################################################################
###############################################################################


def dump_ref(config, ref, recursive=True):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            return db.dump_ref(ref, recursive)


def load_ref(config, ref):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            return db.load_ref(ref)


###############################################################################
# STATUS ######################################################################
###############################################################################


def status(config):
    return {
        "repo": config.get("REPO"),
        "branch": config.get("BRANCH"),
        "user": config.get("USER"),
        "config_dir": config.get("CONFIG_DIR"),
        "project_dir": config.get("PROJECT_DIR") and os.path.abspath(config.get("PROJECT_DIR")),
    }


###############################################################################
# CONFIG ######################################################################
###############################################################################


def config_repo(config, name):
    assert name in with_query(list_repo, "[*].name")(config), f"no such repo: {name}"
    config.REPO = name
    config_branch(config, Ref(DEFAULT_BRANCH).id)


def config_branch(config, name):
    assert name in jsdata(list_branch(config)), f"no such branch: {name}"
    config.BRANCH = name


def config_user(config, user):
    config.USER = user


###############################################################################
# BRANCH ######################################################################
###############################################################################


def current_branch(config):
    return config.BRANCH


def list_branch(config):
    with Repo(config.REPO_PATH) as db:
        with db.tx():
            return sorted([x for x in db.heads()], key=lambda y: y.id)


def list_other_branch(config):
    return [k for k in list_branch(config) if k.id != config.BRANCH]


def create_branch(config, name, commit=None):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            ref = db.head if commit is None else Ref(f"commit/{commit}")
            db.create_branch(Ref(f"head/{name}"), ref)
    config_branch(config, name)


def delete_branch(config, name):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            db.delete_branch(Ref(f"head/{name}"))


def merge_branch(config, name):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            ref = db.merge(db.head().commit, Ref(f"head/{name}")().commit)
            db.checkout(db.set_head(db.head, ref))
        return ref.id


def rebase_branch(config, name):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            ref = db.rebase(Ref(f"head/{name}")().commit, db.head().commit)
            db.checkout(db.set_head(db.head, ref))
        return ref.id


###############################################################################
# DAG #########################################################################
###############################################################################


def list_dags(config):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            dags = Ctx.from_head(db.head).dags
            result = [with_attrs(v, id=v, name=k) for k, v in dags.items()]
            return sorted(result, key=lambda x: x.name)


def delete_dag(config, name, message):
    with Repo(config.REPO_PATH, config.USER, head=config.BRANCHREF) as db:
        with db.tx(True):
            return db.delete_dag(name, message)


def begin_dag(config, *, name=None, message, dump=None):
    with Repo(config.REPO_PATH, config.USER, head=config.BRANCHREF) as db:
        with db.tx(True):
            dag = None if dump is None else db.load_ref(dump)
            return db.begin(name=name, message=message, dag=dag)


def describe_dag(config, ref):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            assert isinstance(ref(), Dag)
            return topology(ref)


def write_dag_html(config, graph):
    with open(Path(__file__).parent / "dag-viz.html") as f:
        html = f.read()
    return html.replace('"REPLACEMENT_TEXT"', json.dumps(jsdata(graph), indent=2))


###############################################################################
# INDEX #######################################################################
###############################################################################


def list_indexes(config):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            return [with_attrs(x, id=x) for x in db.indexes()]


def delete_index(config, index: Ref):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx(True):
            assert isinstance(index(), Index), f"no such index: {index.id}"
            db.delete(index)
    return True


###############################################################################
# API #########################################################################
###############################################################################


def invoke_op(f):
    _, fname = f.__name__.split("_", 1)
    if not hasattr(invoke_op, "fns"):
        invoke_op.fns = {}
    invoke_op.fns[fname] = f
    return f


def format_ops():
    return ", ".join(sorted([*list(invoke_op.fns.keys()), *BUILTIN_FNS.keys()]))


@invoke_op
def op_start_fn(db, index, argv, retry=False, name=None, doc=None):
    with db.tx(True):
        return db.start_fn(index, argv=argv, retry=retry, name=name, doc=doc)


@invoke_op
def op_put_literal(db, index, data, name=None, doc=None):
    with db.tx(True):
        if isinstance(data, Ref) and isinstance(data(), Node):
            return op_set_node(db, index, name, data) if name else data
        nodes = db.extract_nodes(data)
        result = Literal(db.put_datum(data))
        if not len(nodes):
            return db.put_node(result, index=index, name=name, doc=doc)
        else:
            fn = Literal(db.put_datum(Resource("daggerml:build")))
            fn = db.put_node(fn, index=index, name="daggerml:build")
            result = db.put_node(result, index=index)
            nodes = [db.put_node(x.data, index=index, doc=x.doc) for x in nodes]
            result = db.start_fn(index, argv=[fn, result, *nodes], name=name, doc=doc)
            return result


@invoke_op
def op_put_load(db, index, dag, node=None, name=None, doc=None):
    with db.tx(True):
        dag = op_get_dag(db, index, dag) if isinstance(dag, str) else dag
        return db.put_node(Import(dag, node), index=index, name=name, doc=doc)


@invoke_op
def op_commit(db, index, result):
    with db.tx(True):
        return db.commit(res_or_err=result, index=index)


@invoke_op
def op_get_dag(db, index, name=None):
    with db.tx():
        if isinstance(name, str):
            ref = db.get_dag(name)
            assert ref, f"no such dag: {name}"
            return ref
        return name().data.dag


@invoke_op
def op_get_names(db, index, dag: Ref = None):
    with db.tx():
        dag = dag or index().dag
        return dag().names


@invoke_op
def op_get_node(db, index, name, dag: Ref = None):
    with db.tx():
        dag = dag or index().dag
        return dag().names[name]


@invoke_op
def op_set_node(db, index, name, node: Ref):
    with db.tx(True):
        return db.put_node(node, index, name=name)


@invoke_op
def op_get_node_value(db, _, node: Ref):
    with db.tx():
        return db.get_node_value(node)


@invoke_op
def op_get_argv(db, index, dag: Ref = None):
    with db.tx():
        dag = dag() if dag else index().dag()
        return dag.argv


@invoke_op
def op_get_result(db, index, dag: Ref = None):
    with db.tx():
        dag = dag() if dag else index().dag()
        return dag.result or dag.error


@invoke_op
def op_unroll(db, index, node):
    with db.tx():
        return unroll_datum(node().value())


def invoke_api(config, token, data):
    def no_such_op(name):
        def inner(*_args, **_kwargs):
            raise ValueError(f"no such op: {name}")

        return inner

    index = CheckedRef(getattr(token, "to", "NONE"), Index, "invalid token")
    try:
        with Repo(config.REPO_PATH, config.USER, config.BRANCHREF) as db:
            op, args, kwargs = data
            if op in BUILTIN_FNS:
                with db.tx(True):
                    fn = db.put_datum(Resource(f"daggerml:{op}"))
                    fn = op_put_literal(db, index, fn, name=f"daggerml:{op}")
                    argv = [fn, *[op_put_literal(db, index, x) for x in args]]
                return op_start_fn(db, index, argv, **kwargs)
            return invoke_op.fns.get(op, no_such_op(op))(db, index, *args, **kwargs)
    except Exception as e:
        raise Error(e) from e


###############################################################################
# COMMIT ######################################################################
###############################################################################


def list_commit(config):
    with Repo(config.REPO_PATH, head=config.BRANCHREF) as db:
        with db.tx():
            result = [with_attrs(x, id=x) for x in db.commits()]
            return sorted(result, key=lambda x: x.modified, reverse=True)


def commit_log_graph(config):
    @dataclass
    class GNode:
        commit: Ref
        parents: list[Ref]
        children: list[Ref]

    with Repo(config.REPO_PATH, config.USER, head=config.BRANCHREF) as db:
        with db.tx():

            def walk_names(x, head=None):
                if x and x[0]:
                    k = names[x[0]] if x[0] in names else x[0].id
                    tag1 = " HEAD" if head and head.to == db.head.to else ""
                    tag2 = f" {head.id}" if head else ""
                    names[x[0]] = f"{k}{tag1}{tag2}"
                    [walk_names(p) for p in x[1]]

            def walk_nodes(x):
                if x and x[0]:
                    if x[0] not in nodes:
                        parents = [walk_nodes(y) for y in x[1] if y]
                        nodes[x[0]] = AsciiNode(names[x[0]], parents=parents)
                    return nodes[x[0]]

            names = {}
            nodes = {}
            log = dict(asserting(db.log("head")))
            ks = [db.head, *[k for k in log.keys() if k != db.head]]
            [walk_names(log[k], head=k) for k in ks]
            heads = [walk_nodes(log[k]) for k in ks]
            AsciiGraph().show_nodes(heads)


def revert_commit(config, commit):
    raise NotImplementedError("not implemented")


###############################################################################
# UTIL ########################################################################
###############################################################################


@contextmanager
def chdir(path):
    old_path = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(old_path)


def reproducible_tar(dir_, file, *exclude):
    # https://h2.jaguarpaw.co.uk/posts/reproducible-tar/
    # https://www.gnu.org/software/tar/manual/html_section/Reproducibility.html
    # https://reproducible-builds.org/docs/archives/
    tar = some([detect_executable(x, "\\bGNU\\b") for x in ["tar", "gtar"]])
    assert tar, "GNU tar not found on PATH: tried 'tar' and 'gtar'"
    with chdir(dir_):
        proc = subprocess.run(
            [
                tar,
                "--format=posix",
                "--pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime,delete=mtime",
                "--mtime=1970-01-01 00:00:00Z",
                "--sort=name",
                "--numeric-owner",
                "--owner=0",
                "--group=0",
                *[f"--exclude={x}" for x in exclude],
                "-cvf",
                file,
                ".",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    err = "" if not proc.stderr else f"\n{proc.stderr}"
    assert proc.returncode == 0, f"{tar}: exit status: {proc.returncode}{err}"
