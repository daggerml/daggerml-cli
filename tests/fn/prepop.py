import json
import sys
import tempfile

from daggerml_cli._db import DmlDbEnv
from daggerml_cli.ops.dag import DagOps
from daggerml_cli.ops.index import IndexOps
from daggerml_cli.ops.node import NodeOps
from daggerml_cli.types import NAMESPACES

if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
        db = DmlDbEnv.create(tmpdir, namespaces=sorted(NAMESPACES))
        try:
            index_ref = IndexOps(db).create(dump=sys.stdin.read())
            ops = IndexOps(db)
            dag_ops = DagOps(db)
            node_ops = NodeOps(db)
            argv: list[float] = node_ops.unroll(ops.get_argv(index_ref))
            kwargv: dict = node_ops.unroll(ops.get_kwargv(index_ref))
            result = ops.put_literal(index_ref, float(sum(argv[1:]) * kwargv["x"]))
            commit_ref = ops.commit(index_ref, result, message="prepop function result")
            print(json.dumps({"dump": ops.dump(commit_ref)}, separators=(",", ":")))
        finally:
            db.close()
