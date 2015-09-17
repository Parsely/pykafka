# - coding: utf-8 -
__license__ = """
Copyright 2015 Parse.ly, Inc.

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
"""
Author: Keith Bourgoin, Emmett Butler

Protocol implementation for Kafka 0.8

The implementation has been done with an attempt to minimize memory
allocations in order to improve performance. With the exception of
compressed messages, we can calculate the size of the entire message
to send and do only a single memory allocation.

For Reference:

https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

Each message is encoded as either a Request or Response:

RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32

RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest

Response => CorrelationId ResponseMessage
  CorrelationId => int32
  ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse
"""
__all__ = [
    "MetadataRequest", "MetadataResponse", "ProduceRequest", "ProduceResponse",
    "OffsetRequest", "OffsetResponse", "OffsetCommitRequest",
    "FetchRequest", "FetchResponse", "PartitionFetchRequest",
    "OffsetCommitResponse", "OffsetFetchRequest", "OffsetFetchResponse",
    "PartitionOffsetRequest", "ConsumerMetadataRequest",
    "ConsumerMetadataResponse", "PartitionOffsetCommitRequest",
    "PartitionOffsetFetchRequest",
    "Request", "Response", "Message", "MessageSet"
]
import itertools
import logging
import struct
from collections import defaultdict, namedtuple
from zlib import crc32

from .common import CompressionType, Message
from .exceptions import ERROR_CODES, NoMessagesConsumedError
from .utils import Serializable, compression, struct_helpers
from .utils.compat import iteritems, itervalues, buffer


log = logging.getLogger(__name__)


class Request(Serializable):
    """Base class for all Requests. Handles writing header information"""
    HEADER_LEN = 21  # constant for all messages
    CLIENT_ID = b'pykafka'

    def _write_header(self, buff, api_version=0, correlation_id=0):
        """Write the header for an outgoing message.

        :param buff: The buffer into which to write the header
        :type buff: buffer
        :param api_version: The "kafka api version id", used for feature flagging
        :type api_version: int
        :param correlation_id: This is a user-supplied integer. It will be
            passed back in the response by the server, unmodified. It is useful
            for matching request and response between the client and server.
        :type correlation_id: int
        """
        fmt = '!ihhih%ds' % len(self.CLIENT_ID)
        struct.pack_into(fmt, buff, 0,
                         len(buff) - 4,  # msglen excludes this int
                         self.API_KEY,
                         api_version,
                         correlation_id,
                         len(self.CLIENT_ID),
                         self.CLIENT_ID)

    def API_KEY(self):
        """API key for this request, from the Kafka docs"""
        raise NotImplementedError()

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        raise NotImplementedError()


class Response(object):
    """Base class for Response objects."""
    def raise_error(self, err_code, response):
        """Raise an error based on the Kafka error code

        :param err_code: The error code from Kafka
        :param response: The unpacked raw data from the response
        """
        clsname = str(self.__class__).split('.')[-1].split("'")[0]
        raise ERROR_CODES[err_code](
            'Response Type: "%s"\tResponse: %s' % (
                clsname, response))


class Message(Message, Serializable):
    """Representation of a Kafka Message

    NOTE: Compression is handled in the protocol because
          of the way Kafka embeds compressed MessageSets within
          Messages

    Message => Crc MagicByte Attributes Key Value
      Crc => int32
      MagicByte => int8
      Attributes => int8
      Key => bytes
      Value => bytes

    :class:`pykafka.protocol.Message` also contains `partition` and
    `partition_id` fields. Both of these have meaningless default values. When
    :class:`pykafka.protocol.Message` is used by the producer.
    When used in a :class:`pykafka.protocol.FetchRequest`, `partition_id`
    is set to the id of the partition from which the message was sent on
    receipt of the message. In the :class:`pykafka.simpleconsumer.SimpleConsumer`,
    `partition` is set to the :class:`pykafka.partition.Partition` instance
    from which the message was sent.
    """
    MAGIC = 0

    def __init__(self,
                 value,
                 partition_key=None,
                 compression_type=CompressionType.NONE,
                 offset=-1,
                 partition_id=-1,
                 produce_attempt=0):
        self.compression_type = compression_type
        self.partition_key = partition_key
        self.value = value
        self.offset = offset
        # this is set on decode to expose it to clients that use the protocol
        # implementation but not the consumer
        self.partition_id = partition_id
        # self.partition is set by the consumer
        self.partition = None
        self.produce_attempt = produce_attempt

    def __len__(self):
        size = 4 + 1 + 1 + 4 + 4 + len(self.value)
        if self.partition_key is not None:
            size += len(self.partition_key)
        return size

    @classmethod
    def decode(self, buff, msg_offset=-1, partition_id=-1):
        fmt = 'iBBYY'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        crc, _, attr, key, val = response
        # TODO: Handle CRC failure
        return Message(val,
                       partition_key=key,
                       compression_type=attr,
                       offset=msg_offset,
                       partition_id=partition_id)

    def pack_into(self, buff, offset):
        """Serialize and write to ``buff`` starting at offset ``offset``.

        Intentionally follows the pattern of ``struct.pack_into``

        :param buff: The buffer to write into
        :param offset: The offset to start the write at
        """
        if self.partition_key is None:
            fmt = '!BBii%ds' % len(self.value)
            args = (self.MAGIC, self.compression_type, -1,
                    len(self.value), self.value)
        else:
            fmt = '!BBi%dsi%ds' % (len(self.partition_key), len(self.value))
            args = (self.MAGIC, self.compression_type,
                    len(self.partition_key), self.partition_key,
                    len(self.value), self.value)
        struct.pack_into(fmt, buff, offset + 4, *args)
        fmt_size = struct.calcsize(fmt)
        data = buffer(buff[(offset + 4):(offset + 4 + fmt_size)])
        crc = crc32(data) & 0xffffffff
        struct.pack_into('!I', buff, offset, crc)


class MessageSet(Serializable):
    """Representation of a set of messages in Kafka

    This isn't useful outside of direct communications with Kafka, so we
    keep it hidden away here.

    N.B.: MessageSets are not preceded by an int32 like other
          array elements in the protocol.

    MessageSet => [Offset MessageSize Message]
      Offset => int64
      MessageSize => int32

    :ivar messages: The list of messages currently in the MessageSet
    :ivar compression_type: compression to use for the messages
    """
    def __init__(self, compression_type=CompressionType.NONE, messages=None):
        """Create a new MessageSet

        :param compression_type: Compression to use on the messages
        :param messages: An initial list of messages for the set
        """
        self.compression_type = compression_type
        self._messages = messages or []
        self._compressed = None  # compressed Message if using compression

    def __len__(self):
        """Length of the serialized message, in bytes

        We don't put the MessageSetSize in front of the serialization
        because that's *technically* not part of the MessageSet. Most
        requests/responses using MessageSets need that size, though, so
        be careful when using this.
        """
        if self.compression_type == CompressionType.NONE:
            messages = self._messages
        else:
            # The only way to get __len__ of compressed is to compress.
            # Store that so we don't have to do it twice
            if self._compressed is None:
                self._compressed = self._get_compressed()
            messages = [self._compressed]
        return (8 + 4) * len(messages) + sum(len(m) for m in messages)

    @property
    def messages(self):
        # Make sure accessing messages directly clears cached compressed data
        self._compressed = None
        return self._messages

    def _get_compressed(self):
        """Get a compressed representation of all current messages.

        Returns a Message object with correct headers set and compressed
        data in the value field.
        """
        assert self.compression_type != CompressionType.NONE
        tmp_mset = MessageSet(messages=self._messages)
        uncompressed = bytearray(len(tmp_mset))
        tmp_mset.pack_into(uncompressed, 0)
        if self.compression_type == CompressionType.GZIP:
            compressed = compression.encode_gzip(buffer(uncompressed))
        elif self.compression_type == CompressionType.SNAPPY:
            compressed = compression.encode_snappy(buffer(uncompressed))
        else:
            raise TypeError("Unknown compression: %s" % self.compression_type)
        return Message(compressed, compression_type=self.compression_type)

    @classmethod
    def decode(cls, buff, partition_id=-1):
        """Decode a serialized MessageSet."""
        messages = []
        offset = 0
        attempted = False
        while offset < len(buff):
            if len(buff) - offset < 12:
                break
            msg_offset, size = struct.unpack_from('!qi', buff, offset)
            offset += 12
            attempted = True
            if len(buff) - offset < size:
                break
            # TODO: Check we have all the requisite bytes
            message = Message.decode(buff[offset:offset + size],
                                     msg_offset,
                                     partition_id=partition_id)
            # print '[%d] (%s) %s' % (message.offset, message.partition_key, message.value)
            messages.append(message)
            offset += size
        if len(messages) == 0 and attempted:
            raise NoMessagesConsumedError()
        return MessageSet(messages=messages)

    def pack_into(self, buff, offset):
        """Serialize and write to ``buff`` starting at offset ``offset``.

        Intentionally follows the pattern of ``struct.pack_into``

        :param buff: The buffer to write into
        :param offset: The offset to start the write at
        """
        if self.compression_type == CompressionType.NONE:
            messages = self._messages
        else:
            if self._compressed is None:
                self._compressed = self._get_compressed()
            messages = [self._compressed]

        for message in messages:
            mlen = len(message)
            struct.pack_into('!qi', buff, offset, -1, mlen)
            offset += 12
            message.pack_into(buff, offset)
            offset += mlen


##
# Metadata API
##

class MetadataRequest(Request):
    """Metadata Request

    MetadataRequest => [TopicName]
      TopicName => string
    """
    def __init__(self, topics=None):
        """Create a new MetadatRequest

        :param topics: Topics to query. Leave empty for all available topics.
        """
        self.topics = topics or []

    def __len__(self):
        """Length of the serialized message, in bytes"""
        return self.HEADER_LEN + 4 + sum(len(t) + 2 for t in self.topics)

    @property
    def API_KEY(self):
        """API_KEY for this request, from the Kafka docs"""
        return 3

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        struct.pack_into('!i', output, self.HEADER_LEN, len(self.topics))
        offset = self.HEADER_LEN + 4
        for t in self.topics:
            tlen = len(t)
            struct.pack_into('!h%ds' % tlen, output, offset, tlen, t)
            offset += 2 + tlen
        return output


BrokerMetadata = namedtuple('BrokerMetadata', ['id', 'host', 'port'])
TopicMetadata = namedtuple('TopicMetadata', ['name', 'partitions', 'err'])
PartitionMetadata = namedtuple('PartitionMetadata',
                               ['id', 'leader', 'replicas', 'isr', 'err'])


class MetadataResponse(Response):
    """Response from MetadataRequest

    MetadataResponse => [Broker][TopicMetadata]
      Broker => NodeId Host Port
      NodeId => int32
      Host => string
      Port => int32
      TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
      TopicErrorCode => int16
      PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
      PartitionErrorCode => int16
      PartitionId => int32
      Leader => int32
      Replicas => [int32]
      Isr => [int32]
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[iSi] [hS [hii [i] [i] ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        broker_info, topics = response

        self.brokers = {}
        for (id_, host, port) in broker_info:
            self.brokers[id_] = BrokerMetadata(id_, host, port)

        self.topics = {}
        for (err, name, partitions) in topics:
            part_metas = {}
            for (p_err, id_, leader, replicas, isr) in partitions:
                part_metas[id_] = PartitionMetadata(id_, leader, replicas,
                                                    isr, p_err)
            self.topics[name] = TopicMetadata(name, part_metas, err)


##
# Produce API
##

class ProduceRequest(Request):
    """Produce Request

    ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
      RequiredAcks => int16
      Timeout => int32
      Partition => int32
      MessageSetSize => int32
    """
    def __init__(self,
                 compression_type=CompressionType.NONE,
                 required_acks=1,
                 timeout=10000):
        """Create a new ProduceRequest

        ``required_acks`` determines how many acknowledgement the server waits
        for before returning. This is useful for ensuring the replication factor
        of published messages. The behavior is:

            -1: Block until all servers acknowledge
            0: No waiting -- server doesn't even respond to the Produce request
            1: Wait for this server to write to the local log and then return
            2+: Wait for N servers to acknowledge

        :param partition_requests: Iterable of
            :class:`kafka.pykafka.protocol.PartitionProduceRequest` for this request
        :param compression_type: Compression to use for messages
        :param required_acks: see docstring
        :param timeout: timeout (in ms) to wait for the required acks
        """
        # {topic_name: {partition_id: MessageSet}}
        self.msets = defaultdict(
            lambda: defaultdict(
                lambda: MessageSet(compression_type=compression_type)
            ))
        self.required_acks = required_acks
        self.timeout = timeout
        self._message_count = 0  # this optimization is not premature

    def __len__(self):
        """Length of the serialized message, in bytes"""
        size = self.HEADER_LEN + 2 + 4 + 4  # acks + timeout + len(topics)
        for topic, parts in iteritems(self.msets):
            # topic name
            size += 2 + len(topic) + 4  # topic name + len(parts)
            # partition + mset size + len(mset)
            size += sum(4 + 4 + len(mset) for mset in itervalues(parts))
        return size

    @property
    def API_KEY(self):
        """API_KEY for this request, from the Kafka docs"""
        return 0

    @property
    def messages(self):
        """Iterable of all messages in the Request"""
        return itertools.chain.from_iterable(
            mset.messages
            for topic, partitions in iteritems(self.msets)
            for partition_id, mset in iteritems(partitions)
        )

    def add_message(self, message, topic_name, partition_id):
        """Add a list of :class:`kafka.common.Message` to the waiting request

        :param messages: an iterable of :class:`kafka.common.Message` to add
        :param topic_name: the name of the topic to publish to
        :param partition_id: the partition to publish to
        """
        self.msets[topic_name][partition_id].messages.append(message)
        self._message_count += 1

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        struct.pack_into('!hii', output, offset,
                         self.required_acks, self.timeout, len(self.msets))
        offset += 10
        for topic_name, partitions in iteritems(self.msets):
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name),
                             topic_name, len(partitions))
            offset += struct.calcsize(fmt)
            for partition_id, message_set in iteritems(partitions):
                mset_len = len(message_set)
                struct.pack_into('!ii', output, offset, partition_id, mset_len)
                offset += 8
                message_set.pack_into(output, offset)
                offset += mset_len
        return output

    def message_count(self):
        """Get the number of messages across all MessageSets in the request."""
        return self._message_count


ProducePartitionResponse = namedtuple(
    'ProducePartitionResponse',
    ['err', 'offset']
)


class ProduceResponse(Response):
    """Produce Response. Checks to make sure everything went okay.

    ProduceResponse => [TopicName [Partition ErrorCode Offset]]
      TopicName => string
      Partition => int32
      ErrorCode => int16
      Offset => int64
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        # TODO: Handle having produced to a non-existent topic (in client)
        fmt = '[S [ihq] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        self.topics = {}
        for (topic, partitions) in response:
            self.topics[topic] = {}
            for partition in partitions:
                pres = ProducePartitionResponse(partition[1], partition[2])
                self.topics[topic][partition[0]] = pres


##
# Fetch API
##

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

    FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
      ReplicaId => int32
      MaxWaitTime => int32
      MinBytes => int32
      TopicName => string
      Partition => int32
      FetchOffset => int64
      MaxBytes => int32
    """
    def __init__(self, partition_requests=[], timeout=1000, min_bytes=1024):
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

    @property
    def API_KEY(self):
        """API_KEY for this request, from the Kafka docs"""
        return 1

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
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

    FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
      TopicName => string
      Partition => int32
      ErrorCode => int16
      HighwaterMarkOffset => int64
      MessageSetSize => int32
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[S [ihqY] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        self.topics = defaultdict(dict)
        for (topic, partitions) in response:
            for partition in partitions:
                self.topics[topic][partition[0]] = FetchPartitionResponse(
                    partition[2],
                    self._unpack_message_set(partition[3],
                                             partition_id=partition[0]),
                    partition[1]
                )

    def _unpack_message_set(self, buff, partition_id=-1):
        """MessageSets can be nested. Get just the Messages out of it."""
        output = []
        message_set = MessageSet.decode(buff, partition_id=partition_id)
        for message in message_set.messages:
            if message.compression_type == CompressionType.NONE:
                output.append(message)
            elif message.compression_type == CompressionType.GZIP:
                decompressed = compression.decode_gzip(message.value)
                output += self._unpack_message_set(decompressed,
                                                   partition_id=partition_id)
            elif message.compression_type == CompressionType.SNAPPY:
                decompressed = compression.decode_snappy(message.value)
                output += self._unpack_message_set(decompressed,
                                                   partition_id=partition_id)
        return output


##
# Offset API
##

_PartitionOffsetRequest = namedtuple(
    'PartitionOffsetRequest',
    ['topic_name', 'partition_id', 'offsets_before', 'max_offsets']
)


class PartitionOffsetRequest(_PartitionOffsetRequest):
    """Offset request for a specific topic/partition

    :ivar topic_name: Name of the topic to look up
    :ivar partition_id: Id of the partition to look up
    :ivar offsets_before: Retrieve offset information for messages before
                          this timestamp (ms). -1 will retrieve the latest
                          offsets and -2 will retrieve the earliest
                          available offset. If -2,only 1 offset is returned
    :ivar max_offsets: How many offsets to return
    """
    pass


class OffsetRequest(Request):
    """An offset request

    OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
      ReplicaId => int32
      TopicName => string
      Partition => int32
      Time => int64
      MaxNumberOfOffsets => int32
    """
    def __init__(self, partition_requests):
        """Create a new offset request"""
        self._reqs = defaultdict(dict)
        for t in partition_requests:
            self._reqs[t.topic_name][t.partition_id] = (t.offsets_before,
                                                        t.max_offsets)

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + replicaId + len(topics)
        size = self.HEADER_LEN + 4 + 4
        for topic, parts in iteritems(self._reqs):
            # topic name + len(parts)
            size += 2 + len(topic) + 4
            # partition + fetch offset + max bytes => for each partition
            size += (4 + 8 + 4) * len(parts)
        return size

    @property
    def API_KEY(self):
        """API_KEY for this request, from the Kafka docs"""
        return 2

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        struct.pack_into('!ii', output, offset, -1, len(self._reqs))
        offset += 8
        for topic_name, partitions in iteritems(self._reqs):
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name),
                             topic_name, len(partitions))
            offset += struct.calcsize(fmt)
            for pnum, (offsets_before, max_offsets) in iteritems(partitions):
                struct.pack_into('!iqi', output, offset,
                                 pnum, offsets_before, max_offsets)
                offset += 16
        return output


OffsetPartitionResponse = namedtuple(
    'OffsetPartitionResponse',
    ['offset', 'err']
)


class OffsetResponse(Response):
    """An offset response

    OffsetResponse => [TopicName [PartitionOffsets]]
      PartitionOffsets => Partition ErrorCode [Offset]
      Partition => int32
      ErrorCode => int16
      Offset => int64
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[S [ih [q] ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.topics = {}
        for topic_name, partitions in response:
            self.topics[topic_name] = {}
            for partition in partitions:
                self.topics[topic_name][partition[0]] = OffsetPartitionResponse(
                    partition[2], partition[1])


class ConsumerMetadataRequest(Request):
    """A consumer metadata request

    ConsumerMetadataRequest => ConsumerGroup
      ConsumerGroup => string
    """
    def __init__(self, consumer_group):
        """Create a new consumer metadata request"""
        self.consumer_group = consumer_group

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + len(self.consumer_group)
        return self.HEADER_LEN + 2 + len(self.consumer_group)

    @property
    def API_KEY(self):
        """API_KEY for this request, from the Kafka docs"""
        return 10

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        cglen = len(self.consumer_group)
        struct.pack_into('!h%ds' % cglen, output, self.HEADER_LEN, cglen,
                         self.consumer_group)
        return output


class ConsumerMetadataResponse(Response):
    """A consumer metadata response

    ConsumerMetadataResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
      ErrorCode => int16
      CoordinatorId => int32
      CoordinatorHost => string
      CoordinatorPort => int32
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'hiSi'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        error_code = response[0]
        if error_code != 0:
            self.raise_error(error_code, response)
        self.coordinator_id = response[1]
        self.coordinator_host = response[2]
        self.coordinator_port = response[3]


_PartitionOffsetCommitRequest = namedtuple(
    'PartitionOffsetCommitRequest',
    ['topic_name', 'partition_id', 'offset', 'timestamp', 'metadata']
)


class PartitionOffsetCommitRequest(_PartitionOffsetCommitRequest):
    """Offset commit request for a specific topic/partition

    :ivar topic_name: Name of the topic to look up
    :ivar partition_id: Id of the partition to look up
    :ivar offset:
    :ivar timestamp:
    :ivar metadata: arbitrary metadata that should be committed with this offset commit
    """
    pass


class OffsetCommitRequest(Request):
    """An offset commit request

    OffsetCommitRequest => ConsumerGroupId ConsumerGroupGenerationId ConsumerId [TopicName [Partition Offset TimeStamp Metadata]]
      ConsumerGroupId => string
      ConsumerGroupGenerationId => int32
      ConsumerId => string
      TopicName => string
      Partition => int32
      Offset => int64
      TimeStamp => int64
      Metadata => string
    """
    def __init__(self,
                 consumer_group,
                 consumer_group_generation_id,
                 consumer_id,
                 partition_requests=[]):
        """Create a new offset commit request

        :param partition_requests: Iterable of
            :class:`kafka.pykafka.protocol.PartitionOffsetCommitRequest` for
            this request
        """
        self.consumer_group = consumer_group
        self.consumer_group_generation_id = consumer_group_generation_id
        self.consumer_id = consumer_id
        self._reqs = defaultdict(dict)
        for t in partition_requests:
            self._reqs[t.topic_name][t.partition_id] = (t.offset,
                                                        t.timestamp,
                                                        t.metadata)

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + string size + consumer group size
        size = self.HEADER_LEN + 2 + len(self.consumer_group)
        # + generation id + string size + consumer_id size + array length
        size += 4 + 2 + len(self.consumer_id) + 4
        for topic, parts in iteritems(self._reqs):
            # topic name + len(parts)
            size += 2 + len(topic) + 4
            # partition + offset + timestamp => for each partition
            size += (4 + 8 + 8) * len(parts)
            # metadata => for each partition
            for partition, (_, _, metadata) in iteritems(parts):
                size += 2 + len(metadata)
        return size

    @property
    def API_KEY(self):
        """API_KEY for this request, from the Kafka docs"""
        return 8

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output, api_version=1)
        offset = self.HEADER_LEN
        fmt = '!h%dsih%dsi' % (len(self.consumer_group), len(self.consumer_id))
        struct.pack_into(fmt, output, offset,
                         len(self.consumer_group), self.consumer_group,
                         self.consumer_group_generation_id,
                         len(self.consumer_id), self.consumer_id,
                         len(self._reqs))

        offset += struct.calcsize(fmt)
        for topic_name, partitions in iteritems(self._reqs):
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name),
                             topic_name, len(partitions))
            offset += struct.calcsize(fmt)
            for pnum, (poffset, timestamp, metadata) in iteritems(partitions):
                fmt = '!iqq'
                struct.pack_into(fmt, output, offset,
                                 pnum, poffset, timestamp)
                offset += struct.calcsize(fmt)
                metalen = len(metadata) or -1
                fmt = '!h'
                pack_args = [fmt, output, offset, metalen]
                if metalen != -1:
                    fmt += '%ds' % metalen
                    pack_args = [fmt, output, offset, metalen, metadata]
                struct.pack_into(*pack_args)
                offset += struct.calcsize(fmt)
        return output


OffsetCommitPartitionResponse = namedtuple(
    'OffsetCommitPartitionResponse',
    ['err']
)


class OffsetCommitResponse(Response):
    """An offset commit response

    OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
      TopicName => string
      Partition => int32
      ErrorCode => int16
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[S [ih ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.topics = {}
        for topic_name, partitions in response:
            self.topics[topic_name] = {}
            for partition in partitions:
                self.topics[topic_name][partition[0]] = OffsetCommitPartitionResponse(partition[1])


_PartitionOffsetFetchRequest = namedtuple(
    'PartitionOffsetFetchRequest',
    ['topic_name', 'partition_id']
)


class PartitionOffsetFetchRequest(_PartitionOffsetFetchRequest):
    """Offset fetch request for a specific topic/partition

    :ivar topic_name: Name of the topic to look up
    :ivar partition_id: Id of the partition to look up
    """
    pass


class OffsetFetchRequest(Request):
    """An offset fetch request

    OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
      ConsumerGroup => string
      TopicName => string
      Partition => int32
    """
    def __init__(self, consumer_group, partition_requests=[]):
        """Create a new offset fetch request

        :param partition_requests: Iterable of
            :class:`kafka.pykafka.protocol.PartitionOffsetFetchRequest` for
            this request
        """
        self.consumer_group = consumer_group
        self._reqs = defaultdict(list)
        for t in partition_requests:
            self._reqs[t.topic_name].append(t.partition_id)

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + consumer group + len(topics)
        size = self.HEADER_LEN + 2 + len(self.consumer_group) + 4
        for topic, parts in iteritems(self._reqs):
            # topic name + len(parts)
            size += 2 + len(topic) + 4
            # partition => for each partition
            size += 4 * len(parts)
        return size

    @property
    def API_KEY(self):
        """API_KEY for this request, from the Kafka docs"""
        return 9

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output, api_version=1)
        offset = self.HEADER_LEN
        fmt = '!h%dsi' % len(self.consumer_group)
        struct.pack_into(fmt, output, offset,
                         len(self.consumer_group), self.consumer_group,
                         len(self._reqs))
        offset += struct.calcsize(fmt)
        for topic_name, partitions in iteritems(self._reqs):
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name),
                             topic_name, len(partitions))
            offset += struct.calcsize(fmt)
            for pnum in partitions:
                fmt = '!i'
                struct.pack_into(fmt, output, offset, pnum)
                offset += struct.calcsize(fmt)
        return output


OffsetFetchPartitionResponse = namedtuple(
    'OffsetFetchPartitionResponse',
    ['offset', 'metadata', 'err']
)


class OffsetFetchResponse(Response):
    """An offset fetch response

    OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
      TopicName => string
      Partition => int32
      Offset => int64
      Metadata => string
      ErrorCode => int16
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[S [iqSh ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.topics = {}
        for topic_name, partitions in response:
            self.topics[topic_name] = {}
            for partition in partitions:
                pres = OffsetFetchPartitionResponse(partition[1],
                                                    partition[2],
                                                    partition[3])
                self.topics[topic_name][partition[0]] = pres
