import json
import sys
import tempfile
from uuid import uuid4

from daggerml_cli._db import DmlDbEnv
from daggerml_cli.ops.index import IndexOps
from daggerml_cli.types import DEFAULT_HEAD, NAMESPACES, Commit, Head, Tree


def _init_repo(db: DmlDbEnv) -> None:
    with IndexOps(db)._tx(readonly=False) as txn:
        tree_ref = txn.put(Tree(dags={}))
        commit_ref = txn.put(Commit(parents=[], tree=tree_ref, author="test", message="initial"))
        txn.put(Head(commit=commit_ref), to=DEFAULT_HEAD)


if __name__ == "__main__":
    dump_payload = sys.stdin.read()
    with tempfile.TemporaryDirectory(prefix="dml-fn-") as tmpdir:
        db = DmlDbEnv.create(tmpdir, namespaces=sorted(NAMESPACES))
        try:
            _init_repo(db)
            index_ref = IndexOps(db).create(dump=dump_payload)
            ops = IndexOps(db)
            result = ops.put_literal(index_ref, str(uuid4()))
            commit_ref = ops.commit(index_ref, result, message="rand function result")
            print(json.dumps({"dump": ops.dump(commit_ref)}, separators=(",", ":")))
        finally:
            db.close()
