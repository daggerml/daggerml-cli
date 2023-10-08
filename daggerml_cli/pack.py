import msgpack
from base64 import b64encode, b64decode
from daggerml_cli.util import fullname, sort_dict_recursively
from msgpack import ExtType


NXT_CODE = 0
EXT_CODE = {}
EXT_TYPE = {}
EXT_PACK = {}


def next_code():
    global NXT_CODE
    NXT_CODE = NXT_CODE + 1
    return NXT_CODE


def register(cls, pack, unpack):
    code = next_code()
    name = fullname(cls)
    EXT_TYPE[code] = cls
    EXT_CODE[name] = code
    EXT_PACK[code] = [pack, unpack]


def packb(x, hash=False):
    def default(obj):
        code = EXT_CODE.get(fullname(obj))
        if code:
            data = EXT_PACK[code][0](obj, hash)
            return ExtType(code, packb(sort_dict_recursively(data)))
        raise TypeError(f'unknown type: {type(obj)}')
    return msgpack.packb(x, default=default)


def unpackb(x):
    def ext_hook(code, data):
        cls = EXT_TYPE.get(code)
        if cls:
            return cls(*EXT_PACK[code][1](unpackb(data)))
        return ExtType(code, data)
    return msgpack.unpackb(x, ext_hook=ext_hook) if x is not None else None


def packb64(x):
    return b64encode(packb(x)).decode()


def unpackb64(x):
    return unpackb(b64decode(x.encode()))
