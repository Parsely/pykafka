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

import logging
import socket
import struct
from zlib import crc32

from samsa.exceptions import ERROR_CODES
from samsa.utils import attribute_repr
from samsa.utils.namedstruct import NamedStruct
from samsa.utils.structuredio import StructuredBytesIO


logger = logging.getLogger(__name__)


# Socket Utilities

def recvall_into(socket, bytea):
    """
    Reads enough data from the socket to fill the provided bytearray (modifies
    in-place.)

    This is basically a hack around the fact that ``socket.recv_into`` doesn't
    allow buffer offsets.

    :type socket: :class:`socket.Socket`
    :type bytea: ``bytearray``
    :rtype: ``bytearray``
    """
    offset = 0
    size = len(bytea)
    while offset < size:
        remaining = size - offset
        chunk = socket.recv(remaining)
        bytea[offset:(offset+len(chunk))] = chunk
        offset += len(chunk)
    return bytea


def recv_struct(socket, struct):
    """
    Reads enough data from the socket to unpack the given struct.

    :type socket: :class:`socket.Socket`
    :type struct: :class:`struct.Struct`
    :rtype: ``tuple``
    """
    bytea = bytearray(struct.size)
    recvall_into(socket, bytea)
    return struct.unpack_from(buffer(bytea))


def recv_framed(socket, framestruct):
    """
    :type socket: :class:`socket.Socket`
    :type frame: :class:`struct.Struct`
    :rtype: ``bytearray``
    """
    (size,) = recv_struct(socket, framestruct)
    return recvall_into(socket, bytearray(size))


# Message Decoding

MessageSetHeader = NamedStruct('MessageSetHeader', (
    ('i', 'length'),
    ('h', 'error'),
))

def decode_message_sets(payload, from_offsets):
    offset = 0
    for from_offset in from_offsets:
        (length,) = ResponseFrameHeader.unpack_from(payload, offset=offset)
        header = ResponseErrorHeader.unpack_from(payload, offset=offset + ResponseFrameHeader.size)
        if header.error:
            error_class = ERROR_CODES.get(header.error, -1)
            raise error_class(error_class.reason)
        message_set_payload = buffer(payload,
            offset + (ResponseFrameHeader.size + ResponseErrorHeader.size),
            length - ResponseErrorHeader.size)
        yield decode_messages(message_set_payload, from_offset)
        offset += length + ResponseFrameHeader.size

def decode_messages(payload, from_offset):
    """
    Decodes ``Message`` objects from a ``payload`` buffer.
    """
    offset = 0
    while offset < len(payload):
        header = Message.Header.unpack_from(payload, offset)
        length = 4 + header.length
        message = Message(raw=buffer(payload, offset, length), offset=from_offset + offset)
        if len(message) != length:
            if len(message) + offset == len(payload):
                # If this is the last message, it's OK to drop it if it's truncated.
                logger.info('Discarding partial message (expected %s bytes, got %s): %s',
                    length, len(message), message)
            else:
                raise AssertionError("Length of %s (%s) does not match it's "
                    "stated frame size of %s" % (message, len(message), length))
        else:
            yield message
        offset += length


class Message(object):
    __slots__ = ('_headers', 'raw', 'offset')

    Header = NamedStruct('Header', (
        ('i', 'length'),
        ('b', 'magic'),
    ))

    VersionHeaders = {
        0: NamedStruct('Header', (
            ('i', 'checksum'),
        )),
        1: NamedStruct('HeaderWithCompression', (
            ('b', 'compression'),
            ('i', 'checksum'),
        )),
    }

    def __init__(self, raw, offset=0):
        self.raw = raw
        self.offset = offset

        self._headers = []
        header = self.Header.unpack_from(self.raw)
        self._headers.append(header)

        versioned_header = self.VersionHeaders[header.magic].unpack_from(self.raw, offset=self.Header.size)
        self._headers.append(versioned_header)

    __repr__ = attribute_repr('raw', 'offset')

    def __len__(self):
        return len(self.raw)

    def __str__(self):
        return str(self.payload)

    def __getitem__(self, name):
        for header in self._headers:
            try:
                return getattr(header, name)
            except AttributeError:
                pass
        else:
            raise AttributeError('%s does not have a field named "%s".' % (repr(self), name))

    def get(self, name, default=None):
        try:
            return self[name]
        except AttributeError:
            return default

    @property
    def next_offset(self):
        return self.offset + len(self)

    @property
    def payload(self):
        start = self.Header.size + self.VersionHeaders[self['magic']].size
        return self.raw[start:]

    def validate(self):
        if self['checksum'] != crc32(self.payload):
            raise ValueError('invalid checksum')


(REQUEST_TYPE_PRODUCE, REQUEST_TYPE_FETCH, REQUEST_TYPE_MULTIFETCH,
    REQUEST_TYPE_MULTIPRODUCE, REQUEST_TYPE_OFFSETS) = range(0, 5)

OFFSET_LATEST = -1
OFFSET_EARLIEST = -2


def write_request_header(request, topic, partition):
    request.frame(2, topic)
    request.pack(4, partition)
    return request

def encode_message(content):
    magic = 0
    payload = StructuredBytesIO()
    payload.pack(1, magic)
    payload.pack(4, crc32(content))
    payload.write(content)
    return payload.wrap(4)

def encode_messages(messages):
    payload = StructuredBytesIO()
    for message in messages:
        payload.write(encode_message(message))
    return payload.wrap(4)


# Client API

ResponseFrameHeader = struct.Struct('!i')
ResponseErrorHeader = NamedStruct('ResponseErrorHeader', (
    ('h', 'error'),
))

OffsetsResponseHeader = NamedStruct('OffsetsResponseHeader', (
    ('i', 'count'),
))

Offset = NamedStruct('Offset', (
    ('q', 'value'),
))

class Client(object):
    """
    Low-level Kafka protocol client.

    :param host: broker host
    :param port: broker port number
    :param timeout: socket timeout
    """
    def __init__(self, host, port=9092, timeout=None, autoconnect=True):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._socket = None
        if autoconnect:
            self.connect()

    __repr__ = attribute_repr('host', 'port')

    # Socket Management

    def get_socket(self):
        return self._socket

    socket = property(get_socket)

    def connect(self):
        """
        Connect to the broker.
        """
        self._socket = socket.create_connection((self.host, self.port),
            timeout=self.timeout)

    def disconnect(self):
        """
        Disconnect from the Kafka broker.
        """
        try:
            self._socket.close()
        except IOError:
            pass
        finally:
            self._socket = None

    def reconnect(self):
        self.disconnect()
        self.connect()

    def request(self, request):
        # TODO: Retry/reconnect on failure?
        return self.socket.sendall(str(request.wrap(4)))

    def response(self):
        response = recv_framed(self.socket, ResponseFrameHeader)
        header = ResponseErrorHeader.unpack_from(buffer(response))
        if header.error:
            exception_class = ERROR_CODES.get(header.error, -1)
            raise exception_class  # TODO: Add better error messaging.
        return buffer(response, ResponseErrorHeader.size)

    # Protocol Implementation

    def produce(self, topic, partition, messages):
        """
        Sends messages to the broker on a single topic/partition combination.

        >>> client.produce('topic', 0, ('message',))

        :param topic: topic name
        :param partition: partition ID
        :param messages: the messages to be sent
        :type messages: list, generator, or other iterable of strings
        """
        request = StructuredBytesIO()
        request.pack(2, REQUEST_TYPE_PRODUCE)
        write_request_header(request, topic, partition)
        request.write(encode_messages(messages))
        return self.request(request)

    def multiproduce(self, data):
        """
        Sends messages to the broker on multiple topics and/or partitions.

        >>> client.produce((
        ...    ('topic-1', 0, ('message',)),
        ...    ('topic-2', 0, ('message', 'message',)),
        ... ))

        :param data: sequence of 3-tuples of the format ``(topic, partition, messages)``
        :type data: list, generator, or other iterable
        """
        payloads = []
        for topic, partition, messages in data:
            payload = StructuredBytesIO()
            write_request_header(payload, topic, partition)
            payload.write(encode_messages(messages))
            payloads.append(payload)

        request = StructuredBytesIO()
        request.pack(2, REQUEST_TYPE_MULTIPRODUCE)
        request.pack(2, len(payloads))
        for payload in payloads:
            request.write(payload)
        return self.request(request)

    def fetch(self, topic, partition, offset, size):
        """
        Fetches messages from the broker on a single topic/partition.

        >>> for offset, message in client.fetch('test', 0, 0, 1000):
        ...     print offset, message
        0L 'hello world'
        20L 'hello world'

        :param topic: topic name
        :param partition: partition ID
        :param offset: offset to begin read
        :type offset: integer
        :param size: the maximum number of bytes to return
        :rtype: generator of 2-tuples in ``(offset, message)`` format
        """
        # TODO: Document failure modes.
        request = StructuredBytesIO()
        request.pack(2, REQUEST_TYPE_FETCH)
        write_request_header(request, topic, partition)
        request.pack(8, offset)
        request.pack(4, size)
        self.request(request)

        return decode_messages(self.response(), from_offset=offset)

    def multifetch(self, data):
        """
        Fetches messages from the broker on multiple topics/partitions.

        >>> topics = (
        ...     ('topic-1', 0, 0, 1000),
        ...     ('topic-2', 0, 0, 1000),
        ... )
        >>> for i, response in enumerate(client.fetch(topics)):
        ...     print 'response:', i
        ...     for offset, message in messages:
        ...         print offset, message
        response 0
        0L 'hello world'
        20L 'hello world'
        response 1
        0L 'hello world'
        20L 'hello world'

        :param data: sequence of 4-tuples of the format ``(topic, partition, offset, size)``
            For more information, see :meth:`Client.fetch`.
        :rtype: generator of fetch responses (message generators).
            For more information, see :meth:`Client.fetch`.
        """
        payloads = []
        from_offsets = []
        for topic, partition, offset, size in data:
            payload = StructuredBytesIO()
            write_request_header(payload, topic, partition)
            from_offsets.append(offset)
            payload.pack(8, offset)
            payload.pack(4, size)
            payloads.append(payload)

        request = StructuredBytesIO()
        request.pack(2, REQUEST_TYPE_MULTIFETCH)
        request.pack(2, len(payloads))
        for payload in payloads:
            request.write(payload)
        self.request(request)
        return decode_message_sets(self.response(), from_offsets)

    def offsets(self, topic, partition, time, max):
        """
        Returns message offsets before a certain time for the given topic/partition.

        >>> client.offsets('test', 0, OFFSET_EARLIEST, 1)
        [0]

        :param topic: topic name
        :param partition: partition ID
        :param time: the time in milliseconds since the UNIX epoch, or either
            ``OFFSET_EARLIEST`` or ``OFFSET_LATEST``.
        :type time: integer
        :param max: the maximum number of offsets to return
        :rtype: list of offsets
        """
        request = StructuredBytesIO()
        request.pack(2, REQUEST_TYPE_OFFSETS)
        write_request_header(request, topic, partition)
        request.pack(8, time)
        request.pack(4, max)
        self.request(request)
        response = self.response()
        (count,) = OffsetsResponseHeader.unpack_from(response)
        offsets = []
        for i in xrange(0, count):
            offsets.append(Offset.unpack_from(response,
                offset=OffsetsResponseHeader.size + (i * Offset.size)).value)
        return offsets
