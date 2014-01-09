# - coding: utf-8 -
"""Protocol implementation for Kafka 0.8

The implementation has been done with an attempt to minimize memory
allocations in order to improve performance. With the exception of
compressed messages, we can calculate the size of the entire message
to send and do only a single allocation.

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

__license__ = """
Copyright 2014 Parse.ly, Inc.

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
import collections
import itertools
import struct

from collections import defaultdict, namedtuple
from samsa.common import (
    Broker, Cluster, Message, Partition, PartitionMetadata, Topic
)
from samsa.utils import Serializable, compression


def raise_error(err_code):
    raise Exception("Not wholly implemented yet! Err code: %s" % err_code)


class Request(Serializable):
    """Base class for all Requests. Handles writing header information"""
    HEADER_LEN= 19 # constant for all messages
    CLIENT_ID = 'samsa'

    def _write_header(self, buff, api_version=0, correlation_id=0):
        """Write the header for an outgoing message"""
        fmt = '!ihhih%ds' % len(self.CLIENT_ID)
        struct.pack_into(fmt, buff, 0,
                         len(buff) - 4, # msglen excludes this int
                         self.API_KEY,
                         api_version,
                         correlation_id,
                         len(self.CLIENT_ID),
                         self.CLIENT_ID)

    def API_KEY(self):
        """API_KEY for this request, from the Kafka docs"""
        raise NotImplementedError()

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        raise NotImplementedError()


class Response(object):
    """Base class for Response objects."""
    def _unpack(self, fmt_list, buff, offset, count=1):
        # TODO: Replace with calls to utils.unpack_from
        items = []
        for i in xrange(count):
            item = []
            for fmt in fmt_list:
                if type(fmt) == list:
                    count = struct.unpack_from('!i', buff, offset)[0]
                    offset += 4
                    subitems,offset = self._unpack(fmt, buff, offset, count=count)
                    item.append(subitems)
                else:
                    for ch in fmt:
                        if ch == 'S':
                            ch = '%ds' % struct.unpack_from('!h', buff, offset)
                            offset += 2
                        elif ch == 'M':
                            # TODO: should result in buffer, cause this is just bytes
                            ch = '%ds' % struct.unpack_from('!i', buff, offset)
                            offset += 4
                        unpacked = struct.unpack_from('!'+ch, buff, offset)
                        offset += struct.calcsize(ch)
                        item.append(unpacked[0])
            if len(item) == 1:
                items.append(item[0])
            else:
                items.append(tuple(item))
        return items,offset


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
    :ivar compression: compression to use for the messages
    """
    def __init__(self, compression=compression.NONE, messages=None):
        """Create a new MessageSet

        :param compression: Compression to use on the messages
        :param messages: An initial list of messages for the set
        """
        self.compression = compression
        self._messages = messages or []
        self._compressed = None # compressed Message if using compression

    def __len__(self):
        """Length of the serialized message, in bytes

        We don't put the MessageSetSize in front of the serialization
        because that's *technically* not part of the MessageSet. Most
        requests/responses using MessageSets need that size, though, so
        be careful when using this.
        """
        if self.compression == compression.NONE:
            messages = self._messages
        else:
            # The only way to get __len__ of compressed is to compress. Store
            # that so we don't have to do it twice
            if self._compressed is None:
                self._compressed = self._get_compressed()
            messages = [self._compressed]
        return (8+4)*len(messages) + sum(len(m) for m in messages)

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
        assert self.compression != compression.NONE
        tmp_mset = MessageSet(messages=self._messages)
        uncompressed = bytearray(len(tmp_mset))
        tmp_mset.pack_into(uncompressed, 0)
        if self.compression == compression.GZIP:
            compressed = compression.encode_gzip(buffer(uncompressed))
        elif self.compression == compression.SNAPPY:
            compressed = compression.encode_snappy(buffer(uncompressed))
        else:
            raise TypeError("Unknown compression: %s" % self.compression)
        return Message(compressed, compression=self.compression)

    @classmethod
    def decode(cls, buff):
        """Decode a serialized MessageSet."""
        messages = []
        offset = 0
        while offset < len(buff):
            # TODO: Check we have enough bytes to get msg offset/size
            msg_offset,size = struct.unpack_from('!qi', buff, offset)
            offset += 12
            # TODO: Check we have all the requisite bytes
            message = Message.decode(buff[offset:offset+size], msg_offset)
            #print '[%d] (%s) %s' % (message.offset, message.partition_key, message.value)
            messages.append(message)
            offset += size
        return MessageSet(messages=messages)

    def pack_into(self, buff, offset):
        """Serialize and write to ``buff`` starting at offset ``offset``.

        Intentionally follows the pattern of ``struct.pack_into``

        :param buff: The buffer to write into
        :param offset: The offset to start the write at
        """
        if self.compression == compression.NONE:
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
## Metadata API
##

class MetadataRequest(Request):
    """Metadata Request

    MetadataRequest => [TopicName]
      TopicName => string
    """
    def __init__(self, topics=[]):
        """Create a new MetadatRequest

        :param topics: Topics to query. Leave empty for all available topics.
        """
        self.topics = topics

    def __len__(self):
        """Length of the serialized message, in bytes"""
        return self.HEADER_LEN + 4 + sum(len(t)+2 for t in self.topics)

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


class MetadataResponse(Response):
    """Response from MetadataRequest

    The response contains only the topics and brokers received. Putting it into
    a Cluster is separate because it's possible to have a MetadataRequest that's
    only looking at some topics.

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
        fmt = [['iSi'], ['hS', ['hii', ['i'], ['i']]]]
        response,_ = self._unpack(fmt, buff, 0)
        broker_info, topic_info = response[0]

        self.brokers = {}
        for (node_id, host, port) in broker_info:
            self.brokers[node_id] = Broker(node_id, host, port)

        self.topics = {}
        for err, name, partitions in topic_info:
            # TODO: Be sure to check error codes in partitions too
            partition_metas = [
                PartitionMetadata(id_, leader, replicas, isr)
                for (_, id_, leader, replicas, isr) in partitions
            ]
            self.topics[name] = Topic(name, partition_metas, self.brokers)

    def to_cluster(self):
        """Return a Cluster based on the Response"""
        return Cluster(self.brokers, self.topics)


##
## Produce API
##

class ProduceRequest(Request):
    """Produce Request

    ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
      RequiredAcks => int16
      Timeout => int32
      Partition => int32
      MessageSetSize => int32
    """
    def __init__(self,  compression=compression.NONE, required_acks=1, timeout=10000):
        """Create a new ProduceRequest

        ``required_acks`` determines how many acknowledgement the server waits
        for before returning. This is useful for ensuring the replication factor
        of published messages. The behavior is:

            -1: Block until all servers acknowledge
            0: No waiting -- server doesn't even respond to the Produce request
            1: Wait for this server to write to the local log and then return
            2+: Wait for N servers to acknowledge

        :param compression: Compression to use for messages
        :param required_acks: see docstring
        :param timeout: timeout (in ms) to wait for the required acks
        """
        self._msets = defaultdict(lambda: defaultdict(lambda: MessageSet(compression=compression)))
        self.required_acks = required_acks
        self.timeout = timeout

    def __len__(self):
        """Length of the serialized message, in bytes"""
        size = self.HEADER_LEN + 2+4+4 # acks + timeout + len(topics)
        for topic,parts in self._msets.iteritems():
            # topic name
            size += 2 + len(topic) + 4 # topic name + len(parts)
            # partition + mset size + len(mset)
            size += sum(4+4+len(mset) for mset in parts.itervalues())
        return size

    @property
    def API_KEY(self):
        """API_KEY for this request, from the Kafka docs"""
        return 0

    def add_messages(self, messages, topic_name, partition_num):
        """Add a list of :class:`samsa.common.Message` to the waiting request

        :param messages: an iterable of :class:`samsa.common.Message` to add
        :param topic_name: the name of the topic to publish to
        :param partition_num: the partition number to publish to
        """
        self._msets[topic_name][partition_num].messages.extend(messages)

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        struct.pack_into('!hii', output, offset,
                         self.required_acks, self.timeout, len(self._msets))
        offset += 10
        for topic_name, partitions in self._msets.iteritems():
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name), topic_name, len(partitions))
            offset += struct.calcsize(fmt)
            for partition_num,message_set in partitions.iteritems():
                mset_len = len(message_set)
                struct.pack_into('!ii', output, offset, partition_num, mset_len)
                offset += 8
                message_set.pack_into(output, offset)
                offset += mset_len
        return output


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
        fmt = [['S', ['ihq']]]
        response,_ = self._unpack(fmt, buff, 0)
        self.topics = {}
        for (topic,partitions) in response[0]:
            self.topics[topic] = {p[0]: p[2] for p in partitions if p[1] == 0}


##
## Fetch API
##

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
    def __init__(self, timeout=1000, min_bytes=1024):
        """Create a new fetch request

        Kafka 0.8 uses long polling for fetch requests, which is different
        from 0.7x. Instead of polling and waiting, we can now set a timeout
        to wait and a minimum number of bytes to be collected before it
        returns. This way we can block effectively and also ensure good network
        throughput by having fewer, large transfers instead of many small ones
        every time a byte is written to the log.

        :param timeout: Max time to wait (in ms) for a response from the server
        :param min_bytes: Minimum bytes to collect before returning
        """
        self.timeout = timeout
        self.min_bytes = min_bytes
        self._topics = defaultdict(dict)

    def add_fetch(self, topic_name, partition_num, offset, max_bytes=307200):
        """Add a topic/partition/offset to the requesti

        :param topic_name: The topic to fetch from
        :param partition_num: The partition to fetch from
        :param offset: The offset to start reading data from
        :param max_bytes: The maximum number of bytes to return in the response
        """
        self._topics[topic_name][partition_num] = (offset, max_bytes)

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # replica + max wait + min bytes + len(topics)
        size = self.HEADER_LEN + 4+4+4+4
        for topic,parts in self._topics.iteritems():
            # topic name + len(parts)
            size += 2+len(topic) + 4
            # partition + fetch offset + max bytes => for each partition
            size += (4+8+4) * len(parts)
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
                         -1, self.timeout, self.min_bytes, len(self._topics))
        offset += 16
        for topic_name, partitions in self._topics.iteritems():
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name), topic_name, len(partitions))
            offset += struct.calcsize(fmt)
            for partition_num,(fetch_offset,max_bytes) in partitions.iteritems():
                struct.pack_into('!iqi', output, offset,
                                 partition_num, fetch_offset, max_bytes)
                offset += 16
        return output


class FetchPartitionResponse(object):
    """Partition information that's part of a FetchResponse"""
    def __init__(self, max_offset, messages):
        """Create a new FetchPartitionResponse

        :param max_offset: The offset at the end of this partition
        :param messages: Messages in the response
        """
        self.max_offset = max_offset
        self.messages = messages


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
        fmt = [['S', ['ihqM']]]
        response,_ = self._unpack(fmt, buff, 0)
        errors = []
        self.topics = {}
        for (topic,partitions) in response[0]:
            for partition in partitions:
                self.topics[topic] = FetchPartitionResponse(
                    partition[2], self._unpack_message_set(partition[3]),
                )

    def _unpack_message_set(self, buff):
        """MessageSets can be nested. Get just the Messages out of it."""
        output = []
        message_set = MessageSet.decode(buff)
        for message in message_set.messages:
            if message.compression == compression.NONE:
                output.append(message)
            elif message.compression == compression.GZIP:
                decompressed = compression.decode_gzip(message.value)
                output += self._unpack_message_set(decompressed)
            elif message.compression == compression.SNAPPY:
                # Kafka is sending incompatible headers. Strip it off.
                decompressed = compression.decode_snappy(message.value[20:])
                output += self._unpack_message_set(decompressed)
        return output



##
## Offset API
##

class OffsetRequest(Request):
    """An offset request

    OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
      ReplicaId => int32
      TopicName => string
      Partition => int32
      Time => int64
      MaxNumberOfOffsets => int32
    """
    def __init__(self):
        """Create a new offset request"""
        self._topics = defaultdict(dict)

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + replicaId + len(topics)
        size = self.HEADER_LEN + 4+4
        for topic,parts in self._topics.iteritems():
            # topic name + len(parts)
            size += 2+len(topic) + 4
            # partition + fetch offset + max bytes => for each partition
            size += (4+8+4) * len(parts)
        return size

    def add_topic(self, topic_name, partition_num, offsets_before, max_offsets=1):
        """Add a topic/partition to get offset information for

        :param topic_name: Name of the topic to look up
        :param partition_num: Number of the partition to look up
        :param offsets_before: Retrieve offset information for messages before
                               this timestamp (ms). -1 will retrieve the latest
                               offsets and -2 will retrieve the earliest
                               available offset. If -2,only 1 offset is returned
        :param max_offsets: How many offsets to return
        """
        self._topics[topic_name][partition_num] = (offsets_before, max_offsets)

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
        struct.pack_into('!ii', output, offset, -1, len(self._topics))
        offset += 8
        for topic_name, partitions in self._topics.iteritems():
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name), topic_name, len(partitions))
            offset += struct.calcsize(fmt)
            for partition_num,(offsets_before,max_offsets) in partitions.iteritems():
                struct.pack_into('!iqi', output, offset,
                                 partition_num, offsets_before, max_offsets)
                offset += 16
        return output


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
        fmt = [ ['S', ['ih', ['q']]] ]
        response,_ = self._unpack(fmt, buff, 0)

        self.topics = {}
        for topic_name, partitions in response[0]:
            self.topics[topic_name] = {}
            for partition in partitions:
                self.topics[topic_name][partition[0]] = partition[2]
