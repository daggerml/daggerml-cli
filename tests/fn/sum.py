import json
import sys
import tempfile
from typing import cast

from daggerml_cli._db import DmlDbEnv
from daggerml_cli.ops.index import IndexOps
from daggerml_cli.ops.node import NodeOps
from daggerml_cli.types import NAMESPACES, Error

if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
        db = DmlDbEnv.create(tmpdir, namespaces=sorted(NAMESPACES))
        try:
            index_ref = IndexOps(db).create(dump=sys.stdin.read())
            ops = IndexOps(db)
            node_ops = NodeOps(db)
            argv = cast(list, node_ops.unroll(ops.get_argv(index_ref)))
            _, *args = [cast(float, x) for x in argv]
            try:
                for i, arg in enumerate(args):
                    if not isinstance(arg, (int, float)):
                        raise TypeError(f"Argument at index {i} is {type(arg).__name__}, expected int or float")
                result = ops.put_literal(index_ref, float(sum(args)))
            except Exception as e:
                result = Error.from_ex(e)
            commit_ref = ops.commit(index_ref, result, message="sum function result")
            print(json.dumps({"dump": ops.dump(commit_ref)}, separators=(",", ":")))
        finally:
            db.close()
