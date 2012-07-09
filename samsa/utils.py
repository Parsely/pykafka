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
            raise ValueError('Payload length does not match length specified in header')
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
