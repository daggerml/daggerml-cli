import msgpack
from base64 import b64encode, b64decode
from daggerml_cli.util import fullname, sort_dict_recursively
from dataclasses import dataclass, fields
from msgpack import ExtType


NXT_CODE = 2
EXT_CODE = {}
EXT_TYPE = {}


def packb_type(cls):
    global NXT_CODE
    code = NXT_CODE
    NXT_CODE = NXT_CODE + 1
    EXT_TYPE[code] = cls
    EXT_CODE[fullname(cls)] = code
    return dataclass(cls)


def packb(x):
    def default(obj):
        code = EXT_CODE.get(fullname(obj))
        if code:
            data = [getattr(obj, x.name) for x in fields(obj)]
        elif isinstance(obj, set):
            code = 1
            data = sorted(list(obj))
        else:
            raise TypeError(f'unknown type: {type(obj)}')
        return ExtType(code, packb(sort_dict_recursively(data)))
    return msgpack.packb(x, default=default)


def unpackb(x):
    def ext_hook(code, data):
        cls = EXT_TYPE.get(code)
        if cls:
            return cls(*unpackb(data))
        elif code == 1:
            return set(tuple(unpackb(data)))
        return ExtType(code, data)
    return msgpack.unpackb(x, ext_hook=ext_hook) if x is not None else None


def packb64(x):
    return b64encode(packb(x)).decode()


def unpackb64(x):
    return unpackb(b64decode(x.encode()))
