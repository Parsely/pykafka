__license__ = """
Copyright 2012 DISQUS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

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
