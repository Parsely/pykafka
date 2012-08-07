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

import io
from struct import Struct


class StructuredBytesIO(io.BytesIO):
    BYTES = {
        1: Struct('>b'),
        2: Struct('>h'),
        4: Struct('>i'),
        8: Struct('>q'),
    }

    def __len__(self):
        return len(self.getvalue())

    def __str__(self):
        return self.getvalue()

    def write(self, value, *args, **kwargs):
        if isinstance(value, StructuredBytesIO):
            value = str(value)
        return super(StructuredBytesIO, self).write(value, *args, **kwargs)

    def pack(self, size, *args):
        return self.write(self.BYTES[size].pack(*args))

    def unpack(self, size):
        return self.BYTES[size].unpack(self.read(self.BYTES[size].size))[0]

    def frame(self, size, value):
        value = str(value)
        return self.pack(size, len(value)) + self.write(value)

    def unframe(self, size, validate=True):
        length = self.unpack(size)
        value = self.read(int(length))
        if validate and len(value) != length:
            raise ValueError('Payload length does not match length specified '
                'in header')
        return value

    def wrap(self, size):
        envelope = self.__class__()
        envelope.frame(size, self)
        envelope.seek(0)
        return envelope

    def unwrap(self, size, validate=True):
        payload = self.__class__()
        payload.write(self.unframe(size, validate))
        payload.seek(0)
        return payload

    def print_debug(self):
        import string
        offset = self.tell()
        print ''.join(["%02X " % ord(x) for x in self.read()]).strip()
        self.seek(offset)

        def filt(c):
            if ord(c) < 32 or ord(c) > 126 or c not in string.printable:
                return '?'
            return c
        print '  '.join(map(filt, self.read()))

        self.seek(offset)
