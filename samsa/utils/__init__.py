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

import struct


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
        pairs = ('%s=%s' % (attribute, repr(getattr(self, attribute, None)))
            for attribute in attributes)
        return u'<%s.%s at 0x%x: %s>' % (
            cls.__module__, cls.__name__, id(self), ', '.join(pairs))
    return _repr


# TODO: There's got to be a better home for this
def unpack_from(format_list, buff, start_offset):
    def _unpack(fmt_list, offset, count=1):
        items = []
        for i in xrange(count):
            item = []
            for fmt in fmt_list:
                if type(fmt) == list:
                    count = struct.unpack_from('!i', buff, offset)[0]
                    offset += 4
                    subitems,offset = _unpack(fmt, buff, offset, count=count)
                    item.append(subitems)
                else:
                    for ch in fmt:
                        if ch in 'SY':
                            len_fmt = '!h' if ch == 'S' else '!i'
                            len_ = struct.unpack_from(len_fmt, buff, offset)[0]
                            offset += struct.calcsize(len_fmt)
                            if len_ == -1:
                                item.append(None)
                                continue
                            ch = '%ds' % len_
                        unpacked = struct.unpack_from('!'+ch, buff, offset)
                        offset += struct.calcsize(ch)
                        item.append(unpacked[0])
            if len(item) == 1:
                items.append(item[0])
            else:
                items.append(tuple(item))
        return items,offset
    return _unpack(format_list, start_offset)[0]


class Serializable(object):
    def __len__(self):
        """Length of the bytes that will be sent to the Kafka server."""
        raise NotImplementedError()

    def pack_into(self, buff, offset):
        """Pack serialized bytes into buff starting at offset ``offset``"""
        raise NotImplementedError()
