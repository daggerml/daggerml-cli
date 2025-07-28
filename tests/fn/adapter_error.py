#!/usr/bin/env python3

from daggerml_cli.repo import Error, to_json

if __name__ == "__main__":
    print(to_json(Error.from_ex(ValueError("test error"))))
