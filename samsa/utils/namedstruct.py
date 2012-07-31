from collections import namedtuple
from struct import Struct


class NamedStruct(Struct):
    def __init__(self, name, fields, byteorder='!'):
        fmt = byteorder + ''.join(field[0] for field in fields)
        super(NamedStruct, self).__init__(fmt)
        self.namedtuple = namedtuple(name, (field[1] for field in fields))

    def pack(self, *args, **kwargs):
        arguments = self.namedtuple(*args, **kwargs)
        return super(NamedStruct, self).pack(*arguments)

    def pack_into(self, buf, offset, *args, **kwargs):
        arguments = self.namedtuple(*args, **kwargs)
        return super(NamedStruct, self).pack_into(buf, offset, *arguments)

    def unpack(self, *args, **kwargs):
        unpacked = super(NamedStruct, self).unpack(*args, **kwargs)
        return self.namedtuple(*unpacked)

    def unpack_from(self, *args, **kwargs):
        unpacked = super(NamedStruct, self).unpack_from(*args, **kwargs)
        return self.namedtuple(*unpacked)
