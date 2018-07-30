# - coding: utf-8 -
from datetime import datetime
import struct
from pkg_resources import parse_version
from six import integer_types
from zlib import crc32

from ..common import CompressionType, Message
from ..exceptions import MessageSetDecodeFailure
from ..utils import Serializable, struct_helpers, compression
from ..utils.compat import buffer


class Message(Message, Serializable):
    """Representation of a Kafka Message

    NOTE: Compression is handled in the protocol because of the way Kafka embeds
    compressed MessageSets within Messages

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

    N.B.: MessageSets are not preceded by an int32 like other array elements in the
    protocol.

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
