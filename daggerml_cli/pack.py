import msgpack
from base64 import b64encode, b64decode
from daggerml_cli.util import fullname
from dataclasses import dataclass
from msgpack import ExtType


EXT_TYPE = {}
EXT_CODE = {}


def pack_type(code):
    def wrapped(cls):
        EXT_TYPE[code] = cls
        EXT_CODE[fullname(cls)] = code
        return dataclass(slots=True)(cls)
    return wrapped


def packb(x):
    def default(obj):
        code = EXT_CODE.get(fullname(obj))
        if code:
            return ExtType(code, packb([getattr(obj, x) for x in obj.__slots__]))
        raise TypeError(f'unknown type: {type(obj)}')
    return msgpack.packb(x, default=default)


def unpackb(x):
    def ext_hook(code, data):
        cls = EXT_TYPE.get(code)
        if cls:
            return cls(*unpackb(data))
        return ExtType(code, data)
    return msgpack.unpackb(x, ext_hook=ext_hook) if x is not None else None


def packb64(x):
    return b64encode(packb(x)).decode()


def unpackb64(x):
    return unpackb(b64decode(x.encode()))
