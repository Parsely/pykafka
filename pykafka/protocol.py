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

Protocol implementation for Kafka>=0.8.2

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
    "PartitionOffsetRequest", "GroupCoordinatorRequest",
    "GroupCoordinatorResponse", "PartitionOffsetCommitRequest",
    "PartitionOffsetFetchRequest",
    "Request", "Response", "Message", "MessageSet"
]
import itertools
import logging
import struct
from collections import defaultdict, namedtuple
from zlib import crc32
from datetime import datetime
from six import integer_types
from pkg_resources import parse_version


from .common import CompressionType, Message
from .exceptions import ERROR_CODES, MessageSetDecodeFailure
from .utils import Serializable, compression, struct_helpers, ApiVersionAware
from .utils.compat import iteritems, itervalues, buffer


log = logging.getLogger(__name__)


class Request(Serializable, ApiVersionAware):
    """Base class for all Requests. Handles writing header information"""
    HEADER_LEN = 21  # constant for all messages
    CLIENT_ID = b'pykafka'
    API_KEY = -1

    @classmethod
    def get_versions(cls):
        return {}

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

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        raise NotImplementedError()


class Response(ApiVersionAware):
    """Base class for Response objects."""
    API_KEY = -1

    @classmethod
    def get_versions(cls):
        return {}

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

    NOTE: Compression is handled in the protocol because of the way Kafka embeds compressed MessageSets within Messages

    Specification::

        Message => Crc MagicByte Attributes Key Value
          Crc => int32
          MagicByte => int8
          Attributes => int8
          Key => bytes
          Value => bytes

    :class:`pykafka.protocol.Message` also contains `partition` and
    `partition_id` fields. Both of these have meaningless default values. When
    :class:`pykafka.protocol.Message` is used by the producer, `partition_id`
    identifies the Message's destination partition.
    When used in a :class:`pykafka.protocol.FetchRequest`, `partition_id`
    is set to the id of the partition from which the message was sent on
    receipt of the message. In the :class:`pykafka.simpleconsumer.SimpleConsumer`,
    `partition` is set to the :class:`pykafka.partition.Partition` instance
    from which the message was sent.

    :ivar compression_type: The compression algorithm used to generate the message's
        current value. Internal use only - regardless of the algorithm used, this
        will be `CompressionType.NONE` in any publicly accessible `Message`s.
    :ivar partition_key: Value used to assign this message to a particular partition.
    :ivar value: The payload associated with this message
    :ivar offset: The offset of the message
    :ivar partition_id: The id of the partition to which this message belongs
    :ivar delivery_report_q: For use by :class:`pykafka.producer.Producer`
    """

    __slots__ = [
        "compression_type",
        "partition_key",
        "value",
        "offset",
        "partition_id",
        "partition",
        "produce_attempt",
        "delivery_report_q",
        "protocol_version",
        "timestamp"
    ]
    VALID_TS_TYPES = integer_types + (float, type(None))

    def __init__(self,
                 value,
                 partition_key=None,
                 compression_type=CompressionType.NONE,
                 offset=-1,
                 partition_id=-1,
                 produce_attempt=0,
                 protocol_version=0,
                 timestamp=None,
                 delivery_report_q=None):
        self.compression_type = compression_type
        self.partition_key = partition_key
        self.value = value
        self.offset = offset
        if timestamp is None and protocol_version > 0:
            timestamp = datetime.utcnow()
        self.set_timestamp(timestamp)
        # this is set on decode to expose it to clients that use the protocol
        # implementation but not the consumer
        self.partition_id = partition_id
        # self.partition is set by the consumer
        self.partition = None
        self.produce_attempt = produce_attempt
        # delivery_report_q is used by the producer
        self.delivery_report_q = delivery_report_q
        assert protocol_version in (0, 1)
        self.protocol_version = protocol_version

    def __len__(self):
        size = 4 + 1 + 1 + 4 + 4
        if self.value is not None:
            size += len(self.value)
        if self.partition_key is not None:
            size += len(self.partition_key)
        if self.protocol_version > 0 and self.timestamp:
            size += 8
        return size

    @classmethod
    def decode(self, buff, msg_offset=-1, partition_id=-1):
        (crc, protocol_version, attr) = struct_helpers.unpack_from('iBB', buff, 0)
        offset = 6
        timestamp = 0
        if protocol_version > 0:
            (timestamp,) = struct_helpers.unpack_from('Q', buff, offset)
            offset += 8
        (key, val) = struct_helpers.unpack_from('YY', buff, offset)
        # TODO: Handle CRC failure
        return Message(val,
                       partition_key=key,
                       compression_type=attr,
                       offset=msg_offset,
                       protocol_version=protocol_version,
                       timestamp=timestamp,
                       partition_id=partition_id)

    def pack_into(self, buff, offset):
        """Serialize and write to ``buff`` starting at offset ``offset``.

        Intentionally follows the pattern of ``struct.pack_into``

        :param buff: The buffer to write into
        :param offset: The offset to start the write at
        """
        # NB a length of 0 means an empty string, whereas -1 means null
        # Assuming a CreateTime timestamp, not a LogAppendTime.
        len_key = -1 if self.partition_key is None else len(self.partition_key)
        len_value = -1 if self.value is None else len(self.value)
        protocol_version = self.protocol_version
        # Only actually use protocol 1 if timestamp is defined.
        if self.protocol_version == 1 and self.timestamp:
            fmt = '!BBQi%dsi%ds' % (max(len_key, 0), max(len_value, 0))
        else:
            protocol_version = 0
            fmt = '!BBi%dsi%ds' % (max(len_key, 0), max(len_value, 0))
        args = [protocol_version,
                self.compression_type,
                len_key,
                self.partition_key or b"",
                len_value,
                self.value or b""]
        if protocol_version > 0:
            args.insert(2, int(self.timestamp))
        struct.pack_into(fmt, buff, offset + 4, *args)
        fmt_size = struct.calcsize(fmt)
        data = buffer(buff[(offset + 4):(offset + 4 + fmt_size)])
        crc = crc32(data) & 0xffffffff
        struct.pack_into('!I', buff, offset, crc)

    @property
    def timestamp_dt(self):
        """Get the timestamp as a datetime, if valid"""
        if self.timestamp > 0:
            # Assuming a unix epoch
            return datetime.utcfromtimestamp(self.timestamp / 1000.0)

    @timestamp_dt.setter
    def timestamp_dt(self, dt):
        """Set the timestamp from a datetime object"""
        self.timestamp = int(
            1000 * (dt - datetime(1970, 1, 1)).total_seconds())

    def set_timestamp(self, ts):
        if type(ts) in self.VALID_TS_TYPES:
            self.timestamp = ts
        elif type(ts) == datetime:
            self.timestamp_dt = ts
        else:
            raise RuntimeError()


class MessageSet(Serializable):
    """Representation of a set of messages in Kafka

    This isn't useful outside of direct communications with Kafka, so we
    keep it hidden away here.

    N.B.: MessageSets are not preceded by an int32 like other array elements in the protocol.

    Specification::

        MessageSet => [Offset MessageSize Message]
          Offset => int64
          MessageSize => int32
    """
    def __init__(self,
                 compression_type=CompressionType.NONE,
                 messages=None,
                 broker_version='0.9.0'):
        """Create a new MessageSet

        :param compression_type: Compression to use on the messages
        :param messages: An initial list of messages for the set
        :param broker_version: A broker version with which this MessageSet is compatible
        """
        self.compression_type = compression_type
        self._messages = messages or []
        self._compressed = None  # compressed Message if using compression
        self._broker_version = broker_version

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
        elif self.compression_type == CompressionType.LZ4:
            if parse_version(self._broker_version) >= parse_version('0.10.0'):
                compressed = compression.encode_lz4(buffer(uncompressed))
            else:
                compressed = compression.encode_lz4_old_kafka(buffer(uncompressed))
        else:
            raise TypeError("Unknown compression: %s" % self.compression_type)
        protocol_version = max((m.protocol_version for m in self._messages))
        return Message(compressed, compression_type=self.compression_type,
                       protocol_version=protocol_version)

    @classmethod
    def decode(cls, buff, partition_id=-1):
        """Decode a serialized MessageSet."""
        messages = []
        offset = 0
        attempted = False
        while offset < len(buff):
            # if the buffer is not large enough to contain the offset information
            if len(buff) - offset < 12:
                break
            msg_offset, size = struct.unpack_from('!qi', buff, offset)
            offset += 12
            attempted = True
            # if the buffer is not large enough to contain the full message
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
            raise MessageSetDecodeFailure(size)
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

    Specification::

        MetadataRequest => [TopicName]
            TopicName => string
    """
    API_VERSION = 0
    API_KEY = 3

    @classmethod
    def get_versions(cls):
        return {0: MetadataRequest, 1: MetadataRequestV1, 2: MetadataRequestV2,
                3: MetadataRequestV3, 4: MetadataRequestV4, 5: MetadataRequestV5}

    def __init__(self, topics=None, *kwargs):
        """Create a new MetadataRequest

        :param topics: Topics to query. Leave empty for all available topics.
        """
        self.topics = topics or []

    def __len__(self):
        """Length of the serialized message, in bytes"""
        return self.HEADER_LEN + 4 + sum(len(t) + 2 for t in self.topics)

    def _topics_len(self):
        return len(self.topics)

    def _serialize(self):
        output = bytearray(len(self))
        self._write_header(output, api_version=self.API_VERSION)
        struct.pack_into('!i', output, self.HEADER_LEN, self._topics_len())
        offset = self.HEADER_LEN + 4
        for t in self.topics:
            tlen = len(t)
            struct.pack_into('!h%ds' % tlen, output, offset, tlen, t)
            offset += 2 + tlen
        return output, offset

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output, _ = self._serialize()
        return output


class MetadataRequestV1(MetadataRequest):
    API_VERSION = 1

    def _topics_len(self):
        # v1 and higher require a null array, not an empty array, to select all topics
        return len(self.topics) or -1


class MetadataRequestV2(MetadataRequestV1):
    API_VERSION = 2


class MetadataRequestV3(MetadataRequestV2):
    API_VERSION = 3


class MetadataRequestV4(MetadataRequestV3):
    """Metadata Request

    Specification::

    Metadata Request (Version: 4) => [topics] allow_auto_topic_creation
        topics => STRING
        allow_auto_topic_creation => BOOLEAN
    """
    API_VERSION = 4

    def __init__(self, topics=None, allow_topic_autocreation=True):
        """Create a new MetadataRequest

        :param topics: Topics to query. Leave empty for all available topics.
        :param allow_topic_autocreation: If this and the broker config
            'auto.create.topics.enable' are true, topics that don't exist will be created
            by the broker. Otherwise, no topics will be created by the broker.
        """
        super(MetadataRequestV4, self).__init__(topics=topics)
        self.allow_topic_autocreation = allow_topic_autocreation

    def __len__(self):
        return super(MetadataRequestV4, self).__len__() + 1

    def get_bytes(self):
        output, offset = self._serialize()
        struct.pack_into('!b', output, offset, self.allow_topic_autocreation)
        return output


class MetadataRequestV5(MetadataRequestV4):
    API_VERSION = 5


BrokerMetadata = namedtuple('BrokerMetadata', ['id', 'host', 'port'])
BrokerMetadataV1 = namedtuple('BrokerMetadataV1', ['id', 'host', 'port', 'rack'])
TopicMetadata = namedtuple('TopicMetadata', ['name', 'partitions', 'err'])
TopicMetadataV1 = namedtuple('TopicMetadataV1', ['name', 'is_internal', 'partitions',
                                                 'err'])
PartitionMetadata = namedtuple('PartitionMetadata',
                               ['id', 'leader', 'replicas', 'isr', 'err'])
PartitionMetadataV5 = namedtuple('PartitionMetadataV5',
                                 ['id', 'leader', 'replicas', 'isr', 'offline_replicas',
                                  'err'])


class MetadataResponse(Response):
    """Response from MetadataRequest

    Specification::

    Metadata Response (Version: 0) => [brokers] [topic_metadata]
        brokers => node_id host port
            node_id => INT32
            host => STRING
            port => INT32
        topic_metadata => error_code topic [partition_metadata]
            error_code => INT16
            topic => STRING
            partition_metadata => error_code partition leader [replicas] [isr]
                error_code => INT16
                partition => INT32
                leader => INT32
                replicas => INT32
                isr => INT32
    """
    API_KEY = 3

    @classmethod
    def get_versions(cls):
        return {0: MetadataResponse, 1: MetadataResponseV1, 2: MetadataResponseV2,
                3: MetadataResponseV3, 4: MetadataResponseV4, 5: MetadataResponseV5}

    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[iSi] [hS [hii [i] [i] ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        broker_info, topics = response
        self._populate(broker_info, topics)

    def _populate(self,
                  broker_info,
                  topics,
                  controller_id=None,
                  cluster_id=None,
                  throttle_time_ms=0):
        self.throttle_time_ms = throttle_time_ms
        self._build_broker_metas(broker_info)
        self.cluster_id = cluster_id
        self.controller_id = controller_id
        self._build_topic_metas(topics)

    def _build_topic_metas(self, topics):
        self.topics = {}
        for (err, name, partitions) in topics:
            self.topics[name] = TopicMetadata(name,
                                              self._build_partition_metas(partitions),
                                              err)

    def _build_partition_metas(self, partitions):
        part_metas = {}
        for (p_err, id_, leader, replicas, isr) in partitions:
            part_metas[id_] = PartitionMetadata(id_, leader, replicas,
                                                isr, p_err)
        return part_metas

    def _build_broker_metas(self, broker_info):
        self.brokers = {}
        for (id_, host, port) in broker_info:
            self.brokers[id_] = BrokerMetadata(id_, host, port)


class MetadataResponseV1(MetadataResponse):
    """Response from MetadataRequest

    Specification::

    Metadata Response (Version: 1) => [brokers] controller_id [topic_metadata]
        brokers => node_id host port rack
            node_id => INT32
            host => STRING
            port => INT32
            rack => NULLABLE_STRING  (new since v0)
        controller_id => INT32  (new since v0)
        topic_metadata => error_code topic is_internal [partition_metadata]
            error_code => INT16
            topic => STRING
            is_internal => BOOLEAN  (new since v0)
            partition_metadata => error_code partition leader [replicas] [isr]
                error_code => INT16
                partition => INT32
                leader => INT32
                replicas => INT32
                isr => INT32
    """
    def __init__(self, buff):
        fmt = '[iSiS] i [hSb [hii [i] [i] ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        broker_info, controller_id, topics = response
        self._populate(broker_info, topics, controller_id=controller_id)

    def _build_topic_metas(self, topics):
        self.topics = {}
        for (err, name, is_internal, partitions) in topics:
            self.topics[name] = TopicMetadataV1(name,
                                                is_internal,
                                                self._build_partition_metas(partitions),
                                                err)

    def _build_broker_metas(self, broker_info):
        self.brokers = {}
        for (id_, host, port, rack) in broker_info:
            self.brokers[id_] = BrokerMetadataV1(id_, host, port, rack)


class MetadataResponseV2(MetadataResponseV1):
    """Response from MetadataRequest

    Specification::

    Metadata Response (Version: 2) => [brokers] cluster_id controller_id [topic_metadata]
        brokers => node_id host port rack
            node_id => INT32
            host => STRING
            port => INT32
            rack => NULLABLE_STRING
        cluster_id => NULLABLE_STRING  (new since v1)
        controller_id => INT32
        topic_metadata => error_code topic is_internal [partition_metadata]
            error_code => INT16
            topic => STRING
            is_internal => BOOLEAN
            partition_metadata => error_code partition leader [replicas] [isr]
                error_code => INT16
                partition => INT32
                leader => INT32
                replicas => INT32
                isr => INT32
    """
    def __init__(self, buff):
        fmt = '[iSiS] Si [hSb [hii [i] [i] ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        broker_info, cluster_id, controller_id, topics = response
        self._populate(broker_info, topics, controller_id=controller_id,
                       cluster_id=cluster_id)


class MetadataResponseV3(MetadataResponseV2):
    """Response from MetadataRequest

    Specification::

    Metadata Response (Version: 3) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
        throttle_time_ms => INT32  (new since v2)
        brokers => node_id host port rack
            node_id => INT32
            host => STRING
            port => INT32
            rack => NULLABLE_STRING
        cluster_id => NULLABLE_STRING
        controller_id => INT32
        topic_metadata => error_code topic is_internal [partition_metadata]
            error_code => INT16
            topic => STRING
            is_internal => BOOLEAN
            partition_metadata => error_code partition leader [replicas] [isr]
                error_code => INT16
                partition => INT32
                leader => INT32
                replicas => INT32
                isr => INT32
    """
    def __init__(self, buff):
        fmt = 'i [iSiS] Si [hSb [hii [i] [i] ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        throttle_time_ms, broker_info, cluster_id, controller_id, topics = response
        self._populate(broker_info, topics, controller_id=controller_id,
                       cluster_id=cluster_id, throttle_time_ms=throttle_time_ms)


class MetadataResponseV4(MetadataResponseV3):
    """Response from MetadataRequest

    Specification::

    Metadata Response (Version: 4) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
        throttle_time_ms => INT32
        brokers => node_id host port rack
            node_id => INT32
            host => STRING
            port => INT32
            rack => NULLABLE_STRING
        cluster_id => NULLABLE_STRING
        controller_id => INT32
        topic_metadata => error_code topic is_internal [partition_metadata]
            error_code => INT16
            topic => STRING
            is_internal => BOOLEAN
            partition_metadata => error_code partition leader [replicas] [isr]
                error_code => INT16
                partition => INT32
                leader => INT32
                replicas => INT32
                isr => INT32
    """
    pass


class MetadataResponseV5(MetadataResponseV4):
    """Response from MetadataRequest

    Specification::

    Metadata Response (Version: 5) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
        throttle_time_ms => INT32
        brokers => node_id host port rack
            node_id => INT32
            host => STRING
            port => INT32
            rack => NULLABLE_STRING
        cluster_id => NULLABLE_STRING
        controller_id => INT32
        topic_metadata => error_code topic is_internal [partition_metadata]
            error_code => INT16
            topic => STRING
            is_internal => BOOLEAN
            partition_metadata => error_code partition leader [replicas] [isr] [offline_replicas]
                error_code => INT16
                partition => INT32
                leader => INT32
                replicas => INT32
                isr => INT32
                offline_replicas => INT32  (new since v4)
    """
    def __init__(self, buff):
        fmt = 'i [iSiS] Si [hSb [hii [i] [i] [i]] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        throttle_time_ms, broker_info, cluster_id, controller_id, topics = response
        self._populate(broker_info, topics, controller_id=controller_id,
                       cluster_id=cluster_id, throttle_time_ms=throttle_time_ms)

    def _build_partition_metas(self, partitions):
        part_metas = {}
        for (p_err, id_, leader, replicas, isr, offline_replicas) in partitions:
            part_metas[id_] = PartitionMetadataV5(id_, leader, replicas,
                                                  isr, offline_replicas, p_err)
        return part_metas


##
# Produce API
##

class ProduceRequest(Request):
    """Produce Request

    Specification::

        ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
          RequiredAcks => int16
          Timeout => int32
          Partition => int32
          MessageSetSize => int32
    """
    API_KEY = 0

    def __init__(self,
                 compression_type=CompressionType.NONE,
                 required_acks=1,
                 timeout=10000,
                 broker_version='0.9.0'):
        """Create a new ProduceRequest

        ``required_acks`` determines how many acknowledgement the server waits
        for before returning. This is useful for ensuring the replication factor
        of published messages. The behavior is::

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
                lambda: MessageSet(compression_type=compression_type,
                                   broker_version=broker_version)
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

    Specification::

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
        self.throttle_time = struct_helpers.unpack_from("i", buff, offset)
        super(FetchResponseV1, self).__init__(buff, offset + 4,
                                              broker_version=broker_version)


class FetchResponseV2(FetchResponseV1):
    API_VERSION = 2


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

    Specification::

        OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
          ReplicaId => int32
          TopicName => string
          Partition => int32
          Time => int64
          MaxNumberOfOffsets => int32
    """
    API_KEY = 2

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

    Specification::

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


class GroupCoordinatorRequest(Request):
    """A consumer metadata request

    Specification::

        GroupCoordinatorRequest => ConsumerGroup
            ConsumerGroup => string
    """
    API_KEY = 10

    def __init__(self, consumer_group):
        """Create a new group coordinator request"""
        self.consumer_group = consumer_group

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + len(self.consumer_group)
        return self.HEADER_LEN + 2 + len(self.consumer_group)

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


class GroupCoordinatorResponse(Response):
    """A group coordinator response

    Specification::

        GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
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

    Specification::

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
    API_KEY = 8

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

    Specification::

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

    Specification::

        OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
            ConsumerGroup => string
            TopicName => string
            Partition => int32
    """
    API_KEY = 9

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

    Specification::

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


###
# Group Membership API
###
class ConsumerGroupProtocolMetadata(object):
    """
    Protocol specification::

    ProtocolMetadata => Version Subscription UserData
        Version => int16
        Subscription => [Topic]
            Topic => string
        UserData => bytes
    """
    def __init__(self, version=0, topic_names=None, user_data=b"testuserdata"):
        self.version = version
        self.topic_names = topic_names or [b"dummytopic"]
        self.user_data = user_data

    def __len__(self):
        # version + len(topic names)
        size = 2 + 4
        for topic_name in self.topic_names:
            # len(topic_name) + topic_name
            size += 2 + len(topic_name)
        # len(user data) + user data
        size += 4 + len(self.user_data)
        return size

    def get_bytes(self):
        output = bytearray(len(self))
        offset = 0
        fmt = '!hi'
        struct.pack_into(fmt, output, offset, self.version, len(self.topic_names))
        offset += struct.calcsize(fmt)
        for topic_name in self.topic_names:
            fmt = '!h%ds' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name), topic_name)
            offset += struct.calcsize(fmt)
        fmt = '!i%ds' % len(self.user_data)
        struct.pack_into(fmt, output, offset, len(self.user_data), self.user_data)
        offset += struct.calcsize(fmt)
        return output

    @classmethod
    def from_bytestring(cls, buff):
        if len(buff) == 0:
            return cls()
        fmt = 'h [S] Y'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        version = response[0]
        topic_names = response[1]
        user_data = response[2]
        return cls(version, topic_names, user_data)


class JoinGroupRequest(Request):
    """A group join request

    Specification::

    JoinGroupRequest => GroupId SessionTimeout MemberId ProtocolType GroupProtocols
        GroupId => string
        SessionTimeout => int32
        MemberId => string
        ProtocolType => string
        GroupProtocols => [ProtocolName ProtocolMetadata]
            ProtocolName => string
            ProtocolMetadata => bytes
    """
    API_KEY = 11

    def __init__(self,
                 group_id,
                 member_id,
                 topic_name,
                 membership_protocol,
                 session_timeout=30000):
        """Create a new group join request"""
        self.protocol = membership_protocol
        self.group_id = group_id
        self.session_timeout = session_timeout
        self.member_id = member_id
        self.protocol_type = self.protocol.protocol_type
        self.group_protocols = [(self.protocol.protocol_name,
                                 bytes(self.protocol.metadata.get_bytes()))]

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + group id + session timeout
        size = self.HEADER_LEN + 2 + len(self.group_id) + 4
        # + member id + protocol type + len(group protocols)
        size += 2 + len(self.member_id) + 2 + len(self.protocol_type) + 4
        # metadata tuples
        for name, metadata in self.group_protocols:
            size += 2 + len(name) + 4 + len(metadata)
        return size

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!h%dsih%dsh%dsi' % (len(self.group_id), len(self.member_id),
                                   len(self.protocol_type))
        struct.pack_into(fmt, output, offset, len(self.group_id), self.group_id,
                         self.session_timeout, len(self.member_id), self.member_id,
                         len(self.protocol_type), self.protocol_type,
                         len(self.group_protocols))
        offset += struct.calcsize(fmt)
        for protocol_name, protocol_metadata in self.group_protocols:
            fmt = '!h%dsi%ds' % (len(protocol_name), len(protocol_metadata))
            struct.pack_into(fmt, output, offset, len(protocol_name), protocol_name,
                             len(protocol_metadata), protocol_metadata)
            offset += struct.calcsize(fmt)
        return output


class JoinGroupResponse(Response):
    """A group join response

    Specification::

    JoinGroupResponse => ErrorCode GenerationId GroupProtocol LeaderId MemberId Members
        ErrorCode => int16
        GenerationId => int32
        GroupProtocol => string
        LeaderId => string
        MemberId => string
        Members => [MemberId MemberMetadata]
            MemberId => string
            MemberMetadata => bytes
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'hiSSS[SY ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.error_code = response[0]
        self.generation_id = response[1]
        self.group_protocol = response[2]
        self.leader_id = response[3]
        self.member_id = response[4]
        self.members = {_id: ConsumerGroupProtocolMetadata.from_bytestring(meta)
                        for _id, meta in response[5]}


class MemberAssignment(object):
    """
    Protocol specification::

    MemberAssignment => Version PartitionAssignment
        Version => int16
        PartitionAssignment => [Topic [Partition]]
            Topic => string
            Partition => int32
        UserData => bytes
    """
    def __init__(self, partition_assignment, version=1):
        self.version = version
        self.partition_assignment = partition_assignment

    @classmethod
    def from_bytestring(cls, buff):
        if len(buff) == 0:
            return cls(tuple())
        fmt = 'h [S [i ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        version = response[0]
        partition_assignment = response[1]
        return cls(partition_assignment, version=version)

    def __len__(self):
        # version + len(partition assignment)
        size = 2 + 4
        for topic_name, partitions in self.partition_assignment:
            # len(topic_name) + topic_name + len(partitions)
            size += 2 + len(topic_name) + 4
            size += 4 * len(partitions)
        return size

    def get_bytes(self):
        output = bytearray(len(self))
        offset = 0
        fmt = '!hi'
        struct.pack_into(fmt, output, offset, self.version,
                         len(self.partition_assignment))
        offset += struct.calcsize(fmt)
        for topic_name, partitions in self.partition_assignment:
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name), topic_name,
                             len(partitions))
            offset += struct.calcsize(fmt)
            for partition_id in partitions:
                fmt = '!i'
                struct.pack_into(fmt, output, offset, partition_id)
                offset += struct.calcsize(fmt)
        return output


class SyncGroupRequest(Request):
    """A group sync request

    Specification::

    SyncGroupRequest => GroupId GenerationId MemberId GroupAssignment
        GroupId => string
        GenerationId => int32
        MemberId => string
        GroupAssignment => [MemberId MemberAssignment]
            MemberId => string
            MemberAssignment => bytes
    """
    API_KEY = 14

    def __init__(self, group_id, generation_id, member_id, group_assignment):
        """Create a new group join request"""
        self.group_id = group_id
        self.generation_id = generation_id
        self.member_id = member_id
        self.group_assignment = group_assignment

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + len(group id) + group id + generation id
        size = self.HEADER_LEN + 2 + len(self.group_id) + 4
        # + len(member id) + member id + len(group assignment)
        size += 2 + len(self.member_id) + 4
        # group assignment tuples
        for member_id, member_assignment in self.group_assignment:
            # + len(member id) + member id + len(member assignment) + member assignment
            size += 2 + len(member_id) + 4 + len(member_assignment)
        return size

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!h%dsih%dsi' % (len(self.group_id), len(self.member_id))
        struct.pack_into(fmt, output, offset, len(self.group_id), self.group_id,
                         self.generation_id, len(self.member_id), self.member_id,
                         len(self.group_assignment))
        offset += struct.calcsize(fmt)
        for member_id, member_assignment in self.group_assignment:
            assignment_bytes = bytes(member_assignment.get_bytes())
            fmt = '!h%dsi%ds' % (len(member_id), len(assignment_bytes))
            struct.pack_into(fmt, output, offset, len(member_id), member_id,
                             len(assignment_bytes), assignment_bytes)
            offset += struct.calcsize(fmt)
        return output


class SyncGroupResponse(Response):
    """A group sync response

    Specification::

    SyncGroupResponse => ErrorCode MemberAssignment
        ErrorCode => int16
        MemberAssignment => bytes
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'hY'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        self.error_code = response[0]
        self.member_assignment = MemberAssignment.from_bytestring(response[1])


class HeartbeatRequest(Request):
    """A group heartbeat request

    Specification::

    HeartbeatRequest => GroupId GenerationId MemberId
        GroupId => string
        GenerationId => int32
        MemberId => string
    """
    API_KEY = 12

    def __init__(self, group_id, generation_id, member_id):
        """Create a new heartbeat request"""
        self.group_id = group_id
        self.generation_id = generation_id
        self.member_id = member_id

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + len(group id) + group id + generation id
        size = self.HEADER_LEN + 2 + len(self.group_id) + 4
        # + len(member id) + member id
        size += 2 + len(self.member_id)
        return size

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!h%dsih%ds' % (len(self.group_id), len(self.member_id))
        struct.pack_into(fmt, output, offset, len(self.group_id), self.group_id,
                         self.generation_id, len(self.member_id), self.member_id)
        offset += struct.calcsize(fmt)
        return output


class HeartbeatResponse(Response):
    """A group heartbeat response

    Specification::

    HeartbeatResponse => ErrorCode
        ErrorCode => int16
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'h'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        self.error_code = response[0]


class LeaveGroupRequest(Request):
    """A group exit request

    Specification::

    LeaveGroupRequest => GroupId MemberId
        GroupId => string
        MemberId => string
    """
    API_KEY = 13

    def __init__(self, group_id, member_id):
        """Create a new group join request"""
        self.group_id = group_id
        self.member_id = member_id

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + len(group id) + group id
        size = self.HEADER_LEN + 2 + len(self.group_id)
        # + len(member id) + member id
        size += 2 + len(self.member_id)
        return size

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!h%dsh%ds' % (len(self.group_id), len(self.member_id))
        struct.pack_into(fmt, output, offset, len(self.group_id), self.group_id,
                         len(self.member_id), self.member_id)
        offset += struct.calcsize(fmt)
        return output


class LeaveGroupResponse(Response):
    """A group exit response

    Specification::

    LeaveGroupResponse => ErrorCode
        ErrorCode => int16
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'h'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        self.error_code = response[0]


###
# Administrative API
###
class ListGroupsRequest(Request):
    """A list groups request

    Specification::

    ListGroupsRequest =>
    """
    API_KEY = 16

    def get_bytes(self):
        """Create a new list group request"""
        output = bytearray(len(self))
        self._write_header(output)
        return output

    def __len__(self):
        """Length of the serialized message, in bytes"""
        return self.HEADER_LEN


GroupListing = namedtuple(
    'GroupListing',
    ['group_id', 'protocol_type']
)


class ListGroupsResponse(Response):
    """A list groups response

    Specification::

    ListGroupsResponse => ErrorCode Groups
      ErrorCode => int16
      Groups => [GroupId ProtocolType]
        GroupId => string
        ProtocolType => string
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'h [SS]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.error = response[0]
        self.groups = {}
        for group_info in response[1]:
            listing = GroupListing(*group_info)
            self.groups[listing.group_id] = listing


class DescribeGroupsRequest(Request):
    """A describe groups request

    Specification::

    DescribeGroupsRequest => [GroupId]
      GroupId => string
    """
    API_KEY = 15

    def __init__(self, group_ids):
        self.group_ids = group_ids

    def get_bytes(self):
        """Create a new list group request"""
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!i'
        struct.pack_into(fmt, output, offset, len(self.group_ids))
        offset += struct.calcsize(fmt)
        for group_id in self.group_ids:
            fmt = '!h%ds' % len(group_id)
            struct.pack_into(fmt, output, offset, len(group_id), group_id)
            offset += struct.calcsize(fmt)
        return output

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # header + len(group_ids)
        size = self.HEADER_LEN + 4
        for group_id in self.group_ids:
            # len(group_id) + group_id
            size += 2 + len(group_id)
        return size


GroupMember = namedtuple(
    'GroupMember',
    ['member_id', 'client_id', 'client_host', 'member_metadata', 'member_assignment']
)


DescribeGroupResponse = namedtuple(
    'DescribeGroupResponse',
    ['error_code', 'group_id', 'state', 'protocol_type', 'protocol', 'members']
)


class DescribeGroupsResponse(Response):
    """A describe groups response

    Specification::


    DescribeGroupsResponse => [ErrorCode GroupId State ProtocolType Protocol Members]
      ErrorCode => int16
      GroupId => string
      State => string
      ProtocolType => string
      Protocol => string
      Members => [MemberId ClientId ClientHost MemberMetadata MemberAssignment]
        MemberId => string
        ClientId => string
        ClientHost => string
        MemberMetadata => bytes
        MemberAssignment => bytes
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[hSSSS [SSSYY ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.groups = {}
        for group_info in response:
            members = {}
            for member_info in group_info[5]:
                member_metadata = ConsumerGroupProtocolMetadata.from_bytestring(
                    member_info[3])
                member_assignment = MemberAssignment.from_bytestring(member_info[4])
                member = GroupMember(*(member_info[:3] + (member_metadata,
                                                          member_assignment)))
                members[member.member_id] = member
            group = DescribeGroupResponse(*(group_info[:5] + (members,)))
            self.groups[group.group_id] = group


_CreateTopicRequest = namedtuple(
    'CreateTopicRequest',
    ['topic_name', 'num_partitions', 'replication_factor', 'replica_assignment',
     'config_entries']
)


class CreateTopicRequest(_CreateTopicRequest):
    def __new__(cls,
                topic_name,
                num_partitions,
                replication_factor,
                replica_assignment,
                config_entries):
        return super(CreateTopicRequest, cls).__new__(
            cls, topic_name, num_partitions, replication_factor, replica_assignment,
            config_entries)


class CreateTopicsRequest(Request):
    """A create topics request

    Specification::

    CreateTopics Request (Version: 0) => [create_topic_requests] timeout
        create_topic_requests => topic num_partitions replication_factor [replica_assignment] [config_entries]
            topic => STRING
            num_partitions => INT32
            replication_factor => INT16
            replica_assignment => partition [replicas]
                partition => INT32
                replicas => INT32
            config_entries => config_name config_value
                config_name => STRING
                config_value => NULLABLE_STRING
        timeout => INT32
    """
    API_KEY = 19

    def __init__(self, topic_requests, timeout=0):
        self.topic_requests = topic_requests
        self.timeout = timeout

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # header + len(topic_reqs)
        size = self.HEADER_LEN + 4
        for topic_req in self.topic_requests:
            # len(topic_name) + topic_name
            size += 2 + len(topic_req.topic_name)
            # num_partitions + replication_factor + len(replica_assignment)
            size += 4 + 2 + 4
            for partition, replicas in topic_req.replica_assignment:
                # partition + len(replicas) + replicas
                size += 4 + 4 + 4 * len(replicas)
            # len(config_entries)
            size += 4
            for config_name, config_value in topic_req.config_entries:
                # len(config_name) + config_name + len(config_value) + config_value
                size += 2 + len(config_name) + 2 + len(config_value)
        # timeout
        size += 4
        return size

    def get_bytes(self):
        """Create a new create topics request"""
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!i'
        struct.pack_into(fmt, output, offset, len(self.topic_requests))
        offset += struct.calcsize(fmt)
        for topic_req in self.topic_requests:
            fmt = '!h%dsihi' % len(topic_req.topic_name)
            struct.pack_into(fmt, output, offset, len(topic_req.topic_name),
                             topic_req.topic_name, topic_req.num_partitions,
                             topic_req.replication_factor,
                             len(topic_req.replica_assignment))
            offset += struct.calcsize(fmt)
            for partition, replicas in topic_req.replica_assignment:
                fmt = '!ii'
                struct.pack_into(fmt, output, offset, partition, len(replicas))
                offset += struct.calcsize(fmt)
                for replica in replicas:
                    fmt = '!i'
                    struct.pack_into(fmt, output, offset, replica)
                    offset += struct.calcsize(fmt)
            fmt = '!i'
            struct.pack_into(fmt, output, offset, len(topic_req.config_entries))
            offset += struct.calcsize(fmt)
            for config_name, config_value in topic_req.config_entries:
                fmt = '!h%dsh%ds' % (len(config_name), len(config_value))
                struct.pack_into(fmt, output, offset, len(config_name), config_name,
                                 len(config_value), config_value)
                offset += struct.calcsize(fmt)
        fmt = '!i'
        struct.pack_into(fmt, output, offset, self.timeout)
        offset += struct.calcsize(fmt)
        return output


class CreateTopicsResponse(Response):
    """A create topics response

    Specification::

    CreateTopics Response (Version: 0) => [topic_errors]
        topic_errors => topic error_code
            topic => STRING
            error_code => INT16
    """
    API_KEY = 19

    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[Sh]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        for _, error_code in response:
            if error_code != 0:
                self.raise_error(error_code, response)


class DeleteTopicsRequest(Request):
    """A delete topics request

    Specification::

    DeleteTopics Request (Version: 0) => [topics] timeout
        topics => STRING
        timeout => INT32
    """
    API_KEY = 20

    def __init__(self, topics, timeout=0):
        self.topics = topics
        self.timeout = timeout

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # header + len(topics)
        size = self.HEADER_LEN + 4
        for topic in self.topics:
            # len(topic) + group_id
            size += 2 + len(topic)
        # timeout
        size += 4
        return size

    def get_bytes(self):
        """Create a new delete topics request"""
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!i'
        struct.pack_into(fmt, output, offset, len(self.topics))
        offset += struct.calcsize(fmt)
        for topic in self.topics:
            fmt = '!h%ds' % len(topic)
            struct.pack_into(fmt, output, offset, len(topic), topic)
            offset += struct.calcsize(fmt)
        fmt = '!i'
        struct.pack_into(fmt, output, offset, self.timeout)
        offset += struct.calcsize(fmt)
        return output


class DeleteTopicsResponse(Response):
    """A delete topics response

    Specification::

    DeleteTopics Response (Version: 0) => [topic_error_codes]
        topic_error_codes => topic error_code
            topic => STRING
            error_code => INT16
    """
    API_KEY = 20

    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[Sh]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        for _, error_code in response:
            if error_code != 0:
                self.raise_error(error_code, response)


class ApiVersionsRequest(Request):
    """An api versions request

    Specification::

        ApiVersions Request (Version: 0) =>
    """
    API_KEY = 18

    def get_bytes(self):
        """Create a new api versions request"""
        output = bytearray(len(self))
        self._write_header(output)
        return output

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # header
        size = self.HEADER_LEN
        return size


ApiVersionsSpec = namedtuple('ApiVersionsSpec', ['key', 'min', 'max'])


class ApiVersionsResponse(Response):
    """
    Specification::

    ApiVersions Response (Version: 0) => error_code [api_versions]
        error_code => INT16
        api_versions => api_key min_version max_version
            api_key => INT16
            min_version => INT16
            max_version => INT16
    """
    API_KEY = 18

    @classmethod
    def get_versions(cls):
        return {0: ApiVersionsResponse, 1: ApiVersionsResponseV1}

    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'h [hhh]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.api_versions = {}
        for api_key, min_v, max_v in response[1]:
            self.api_versions[api_key] = ApiVersionsSpec(api_key, min_v, max_v)


class ApiVersionsResponseV1(ApiVersionsResponse):
    """
    Specification::

    ApiVersions Response (Version: 1) => error_code [api_versions] throttle_time_ms
        error_code => INT16
        api_versions => api_key min_version max_version
            api_key => INT16
            min_version => INT16
            max_version => INT16
        throttle_time_ms => INT32
    """
    def __init__(self, buff):
        """Deserialize into a new Response

        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'h [hhh]i'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.api_versions = {}
        for api_key, min_v, max_v in response[1]:
            self.api_versions[api_key] = ApiVersionsSpec(api_key, min_v, max_v)
        self.throttle_time = response[2]


# Hardcoded API version specifiers for brokers that don't support ApiVersionsRequest
API_VERSIONS_080 = {
    0: ApiVersionsSpec(0, 0, 0),
    1: ApiVersionsSpec(1, 0, 0),
    2: ApiVersionsSpec(2, 0, 0),
    3: ApiVersionsSpec(3, 0, 0),
    4: ApiVersionsSpec(4, 0, 0),
    5: ApiVersionsSpec(5, 0, 0),
    6: ApiVersionsSpec(6, 0, 0),
    7: ApiVersionsSpec(7, 0, 0),
    8: ApiVersionsSpec(8, 0, 1),
    9: ApiVersionsSpec(9, 0, 0),
    10: ApiVersionsSpec(10, 0, 0),
    11: ApiVersionsSpec(11, 0, 0),
    12: ApiVersionsSpec(12, 0, 0),
    13: ApiVersionsSpec(13, 0, 0),
    14: ApiVersionsSpec(14, 0, 0),
    15: ApiVersionsSpec(15, 0, 0),
    16: ApiVersionsSpec(16, 0, 0)
}
API_VERSIONS_090 = {
    0: ApiVersionsSpec(0, 0, 0),
    1: ApiVersionsSpec(1, 0, 1),
    2: ApiVersionsSpec(2, 0, 0),
    3: ApiVersionsSpec(3, 0, 0),
    4: ApiVersionsSpec(4, 0, 0),
    5: ApiVersionsSpec(5, 0, 0),
    6: ApiVersionsSpec(6, 0, 0),
    7: ApiVersionsSpec(7, 0, 0),
    8: ApiVersionsSpec(8, 0, 1),
    9: ApiVersionsSpec(9, 0, 0),
    10: ApiVersionsSpec(10, 0, 0),
    11: ApiVersionsSpec(11, 0, 0),
    12: ApiVersionsSpec(12, 0, 0),
    13: ApiVersionsSpec(13, 0, 0),
    14: ApiVersionsSpec(14, 0, 0),
    15: ApiVersionsSpec(15, 0, 0),
    16: ApiVersionsSpec(16, 0, 0)
}
