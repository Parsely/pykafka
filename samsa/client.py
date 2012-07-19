import socket
from zlib import crc32

from samsa.exceptions import ERROR_CODES
from samsa.utils import attribute_repr
from samsa.utils.structuredio import StructuredBytesIO


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

def decode_message(value):
    payload = value.unwrap(4)
    magic = payload.unpack(1)
    if magic == 0:
        compression = None
    elif magic == 1:
        compression = payload.unpack(1)
    else:
        raise ValueError('Message is of an unknown version or corrupt')

    if compression:
        raise NotImplementedError  # TODO: Implement compression.

    crc = payload.unpack(4)
    content = payload.read()
    if crc != crc32(content):
        raise ValueError('Message failed CRC check')
    return content

def decode_messages(value):
    length = len(value)
    offset = value.tell()
    while offset < length:
        yield offset, decode_message(value)
        offset = value.tell()


# Client API

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
        self.close()
        self.connect()

    def request(self, request):
        # TODO: Retry/reconnect on failure?
        return self.socket.sendall(str(request.wrap(4)))

    def response(self):
        # TODO: Read this to a buffered stream instead.
        length = self.recvall(4).unpack(4)
        response = self.recvall(length)
        error = response.unpack(2)
        if error != 0:
            exception_class = ERROR_CODES.get(error, -1)
            raise exception_class  # TODO: Add better error messaging.
        return StructuredBytesIO(response.read())

    def recvall(self, size):
        # TODO: Retry/reconnect on failure?
        response = StructuredBytesIO()
        while len(response) < size:  # O(1) because this is actually a string
            response.write(self.socket.recv(size - len(response)))
        response.seek(0)
        return response

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
        response = self.response()
        return decode_messages(response)

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
        for topic, partition, offset, size in data:
            payload = StructuredBytesIO()
            write_request_header(payload, topic, partition)
            payload.pack(8, offset)
            payload.pack(4, size)
            payloads.append(payload)

        request = StructuredBytesIO()
        request.pack(2, REQUEST_TYPE_MULTIFETCH)
        request.pack(2, len(payloads))
        for payload in payloads:
            request.write(payload)
        self.request(request)
        response = self.response()

        while response.tell() < len(response):
            # TODO: Extract out fetch response processing into something reusable.
            size = response.unpack(4)
            error = response.unpack(2)
            if error != 0:
                raise NotImplementedError  # TODO
            content = response.read(size - 2)
            block = StructuredBytesIO(content)
            yield decode_messages(block)

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
        offsets = []
        for i in xrange(0, response.unpack(4)):
            offsets.append(response.unpack(8))
        return offsets
