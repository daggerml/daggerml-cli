from base64 import b64decode, b64encode
from zlib import compress, decompress

import msgpack
from msgpack import ExtType

from daggerml_cli.util import asserting, fullname, sort_dict_recursively

NXT_CODE = 0
EXT_CODE = {}
EXT_TYPE = {}
EXT_PACK = {}


def next_code():
    global NXT_CODE  # noqa: PLW0603
    NXT_CODE = NXT_CODE + 1
    return NXT_CODE


def register(cls, pack, unpack):
    code = next_code()
    name = fullname(cls)
    EXT_TYPE[code] = cls
    EXT_CODE[name] = code
    EXT_PACK[code] = [pack, unpack]


def packb(x, hash=False) -> bytes:
    def default(obj):
        code = EXT_CODE.get(fullname(obj))
        if code:
            data = EXT_PACK[code][0](obj, hash)
            return ExtType(code, packb(sort_dict_recursively(data)))
        raise TypeError(f"unknown type: {type(obj)}")

    return asserting(msgpack.packb(x, default=default))


def unpackb(x):
    def ext_hook(code, data):
        cls = EXT_TYPE.get(code)
        if cls:
            return cls(*EXT_PACK[code][1](unpackb(data)))
        return ExtType(code, data)

    return msgpack.unpackb(x, ext_hook=ext_hook) if x is not None else None


def packb64(x, zlib=False):
    return b64encode(compress(packb(x), level=9) if zlib else packb(x)).decode()


def unpackb64(x, zlib=False):
    return unpackb(decompress(b64decode(x.encode())) if zlib else b64decode(x.encode()))
