import io
from struct import Struct


def attribute_repr(*attributes):
    """
    Provides an alternative ``__repr__`` implementation that adds the values of
    the given attributes to the output as a development and debugging aid.

    For example::

        >>> class Foo(object):
        ...     __repr__ = attribute_repr('pk', 'slug')
        <foo.models.Foo at 0x100614c50: pk=1, slug=foo>

    :param \*attributes: a sequence of strings that will be used for attribute
        dereferencing on ``self``.
    """
    def _repr(self):
        cls = self.__class__
        pairs = ('%s=%s' % (attribute, repr(getattr(self, attribute, None))) for attribute in attributes)
        return u'<%s.%s at 0x%x: %s>' % (cls.__module__, cls.__name__, id(self), ', '.join(pairs))
    return _repr


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
