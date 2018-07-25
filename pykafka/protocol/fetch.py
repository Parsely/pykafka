# - coding: utf-8 -
import struct
from collections import namedtuple, defaultdict
from pkg_resources import parse_version

from .base import Request, Response
from .message import MessageSet
from ..common import CompressionType
from ..utils import struct_helpers, compression
from ..utils.compat import iteritems


_PartitionFetchRequest = namedtuple(
    'PartitionFetchRequest',
    ['topic_name', 'partition_id', 'offset', 'max_bytes']
)


class PartitionFetchRequest(_PartitionFetchRequest):
    """Fetch request for a specific topic/partition

    :ivar topic_name: Name of the topic to fetch from
    :ivar partition_id: Id of the partition to fetch from
    :ivar offset: Offset at which to start reading
    :ivar max_bytes: Max bytes to read from this partition (default: 300kb)
    """
    def __new__(cls, topic, partition, offset, max_bytes=1024 * 1024):
        return super(PartitionFetchRequest, cls).__new__(
            cls, topic, partition, offset, max_bytes)


class FetchRequest(Request):
    """A Fetch request sent to Kafka

    Specification::

        FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
          ReplicaId => int32
          MaxWaitTime => int32
          MinBytes => int32
          TopicName => string
          Partition => int32
          FetchOffset => int64
          MaxBytes => int32
    """
    API_KEY = 1

    @classmethod
    def get_versions(cls):
        return {0: FetchRequest, 1: FetchRequest}

    def __init__(self, partition_requests=[], timeout=1000, min_bytes=1024,
                 api_version=0):
        """Create a new fetch request

        Kafka 0.8 uses long polling for fetch requests, which is different
        from 0.7x. Instead of polling and waiting, we can now set a timeout
        to wait and a minimum number of bytes to be collected before it
        returns. This way we can block effectively and also ensure good network
        throughput by having fewer, large transfers instead of many small ones
        every time a byte is written to the log.

        :param partition_requests: Iterable of
            :class:`kafka.pykafka..protocol.PartitionFetchRequest` for this request
        :param timeout: Max time to wait (in ms) for a response from the server
        :param min_bytes: Minimum bytes to collect before returning
        """
        self.timeout = timeout
        self.min_bytes = min_bytes
        self._reqs = defaultdict(dict)
        self.api_version = api_version
        for req in partition_requests:
            self.add_request(req)

    def add_request(self, partition_request):
        """Add a topic/partition/offset to the requests

        :param topic_name: The topic to fetch from
        :param partition_id: The partition to fetch from
        :param offset: The offset to start reading data from
        :param max_bytes: The maximum number of bytes to return in the response
        """
        pr = partition_request
        self._reqs[pr.topic_name][pr.partition_id] = (pr.offset, pr.max_bytes)

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # replica + max wait + min bytes + len(topics)
        size = self.HEADER_LEN + 4 + 4 + 4 + 4
        for topic, parts in iteritems(self._reqs):
            # topic name + len(parts)
            size += 2 + len(topic) + 4
            # partition + fetch offset + max bytes => for each partition
            size += (4 + 8 + 4) * len(parts)
        return size

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output, api_version=self.api_version)
        offset = self.HEADER_LEN
        struct.pack_into('!iiii', output, offset,
                         -1, self.timeout, self.min_bytes, len(self._reqs))
        offset += 16
        for topic_name, partitions in iteritems(self._reqs):
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(
                fmt, output, offset, len(topic_name), topic_name,
                len(partitions)
            )
            offset += struct.calcsize(fmt)
            for partition_id, (fetch_offset, max_bytes) in iteritems(partitions):
                struct.pack_into('!iqi', output, offset,
                                 partition_id, fetch_offset, max_bytes)
                offset += 16
        return output


FetchPartitionResponse = namedtuple(
    'FetchPartitionResponse',
    ['max_offset', 'messages', 'err']
)


class FetchResponse(Response):
    """Unpack a fetch response from the server

    Specification::

        FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
          TopicName => string
          Partition => int32
          ErrorCode => int16
          HighwaterMarkOffset => int64
          MessageSetSize => int32
    """
    API_VERSION = 0
    API_KEY = 1

    @classmethod
    def get_versions(cls):
        return {0: FetchResponse, 1: FetchResponseV1, 2: FetchResponseV2}

    def __init__(self, buff, offset=0, broker_version='0.9.0'):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        :param offset: Offset into the message
        :type offset: int
        """
        fmt = '[S [ihqY] ]'
        response = struct_helpers.unpack_from(fmt, buff, offset)
        self.topics = defaultdict(dict)
        for (topic, partitions) in response:
            for partition in partitions:
                self.topics[topic][partition[0]] = FetchPartitionResponse(
                    partition[2],
                    self._unpack_message_set(partition[3],
                                             partition_id=partition[0],
                                             broker_version=broker_version),
                    partition[1]
                )

    def _unpack_message_set(self, buff, partition_id=-1, broker_version='0.9.0'):
        """MessageSets can be nested. Get just the Messages out of it."""
        output = []
        message_set = MessageSet.decode(buff, partition_id=partition_id)
        for message in message_set.messages:
            if message.compression_type == CompressionType.NONE:
                output.append(message)
                continue
            elif message.compression_type == CompressionType.GZIP:
                decompressed = compression.decode_gzip(message.value)
                messages = self._unpack_message_set(decompressed,
                                                    partition_id=partition_id)
            elif message.compression_type == CompressionType.SNAPPY:
                decompressed = compression.decode_snappy(message.value)
                messages = self._unpack_message_set(decompressed,
                                                    partition_id=partition_id)
            elif message.compression_type == CompressionType.LZ4:
                if parse_version(broker_version) >= parse_version('0.10.0'):
                    decompressed = compression.decode_lz4(message.value)
                else:
                    decompressed = compression.decode_lz4_old_kafka(message.value)
                messages = self._unpack_message_set(decompressed,
                                                    partition_id=partition_id)
            if messages[-1].offset < message.offset:
                # With protocol 1, offsets from compressed messages start at 0
                assert messages[0].offset == 0
                delta = message.offset - len(messages) + 1
                for msg in messages:
                    msg.offset += delta
            output += messages
        return output


class FetchResponseV1(FetchResponse):
    API_VERSION = 1

    def __init__(self, buff, offset=0, broker_version='0.9.0'):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        :param offset: Offset into the message
        :type offset: int
        """
        # TODO: Use throttle_time
        self.throttle_time = struct_helpers.unpack_from("i", buff, offset)[0]
        super(FetchResponseV1, self).__init__(buff, offset + 4,
                                              broker_version=broker_version)


class FetchResponseV2(FetchResponseV1):
    API_VERSION = 2
