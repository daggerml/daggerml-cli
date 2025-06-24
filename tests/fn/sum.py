import json
import os
import sys
from uuid import uuid4

from daggerml_cli.repo import Error
from tests.util import SimpleApi

if __name__ == "__main__":
    js = json.loads(sys.stdin.read())
    cache_key = js["cache_key"]
    dump = js["dump"]
    filter_args = os.getenv("DML_FN_FILTER_ARGS", "")
    fnc_dir = os.getenv("DML_FN_CACHE_DIR", "")

    with SimpleApi.begin("test", "test", cache_path=js["cache_path"], fn_cache_dir=fnc_dir, dump=dump) as d0:
        _, *args = d0.unroll(d0.get_argv())
        args = filter(lambda x: isinstance(x, int), args) if filter_args else args
        uuid = d0.put_literal(uuid4().hex, name="uuid")
        try:
            n0 = d0.put_literal([uuid, sum(args)], name="sum")
        except Exception as e:
            n0 = Error(e)
        result = d0.commit(n0)
        print(result)
