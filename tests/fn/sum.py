try:
    import os
    import sys
    from uuid import uuid4

    from daggerml_cli import repo
    from daggerml_cli.repo import Error
    from daggerml_cli.util import readfile, writefile
    from tests.util import SimpleApi

    cache_key, dump = repo.from_json(sys.stdin.read())
    filter_args = os.getenv('DML_FN_FILTER_ARGS', '')
    cache_dir = os.getenv('DML_FN_CACHE_DIR', '')
    cache_file = os.path.join(cache_dir, cache_key) if cache_dir else None
    result = readfile(cache_file)

    if result:
        print(result)
    else:
        with SimpleApi.begin('test', 'test', config_dir=cache_dir, dump=dump) as d0:
            _, *args = d0.unroll(d0.get_argv())
            args = filter(lambda x: isinstance(x, int), args) if filter_args else args
            try:
                n0 = d0.put_literal([uuid4().hex, sum(args)])
            except Exception as e:
                n0 = Error(e)
            result = d0.commit(n0)
            writefile(result, cache_file)
            print(result)
except Exception as e:
    print(repo.to_json(Error(e)))
