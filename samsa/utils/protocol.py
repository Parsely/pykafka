# - coding: utf-8 -
"""Protocol implementation for Kafka 0.8

TODO: Note about how bytearray sizing is calculated
NOTE: msg size = int32; str = len + int16

TODO: Note about versioning in the future

TODO: Error code checking *everywhere*

"""
import logging
import collections
import itertools
import struct

from collections import defaultdict, namedtuple
from samsa.common import (
    Cluster, Broker, Message, Partition, PartitionMetadata, Topic
)
from samsa.utils import Serializable

def raise_error(err_code):
    raise Exception("Not wholly implemented yet! Err code: %s" % err_code)

class Request(Serializable):
    HEADER_LEN= 19
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
        """Serialize to a message to send to Kafka"""
        raise NotImplementedError()


class Response(object):
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
    """
    def __init__(self, messages=None):
        self.messages = messages or []

    def __len__(self):
        """Returns serialized length of MessageSet.

        We don't put the MessageSetSize in front of the serialization
        because that's *technically* not part of the MessageSet. Most
        requests/responses using MessageSets need that size, though, so
        be careful when using this.
        """
        return (8+4)*len(self.messages) + sum(len(m) for m in self.messages)

    @classmethod
    def decode(cls, buff):
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
        for message in self.messages:
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
        self.topics = topics

    def __len__(self):
        return self.HEADER_LEN + 4 + sum(len(t)+2 for t in self.topics)

    @property
    def API_KEY(self):
        return 3

    def get_bytes(self):
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
    def __init__(self,  required_acks=1, timeout=1000):
        self._msets = defaultdict(lambda: defaultdict(MessageSet))
        self.required_acks = required_acks
        self.timeout = timeout

    def __len__(self):
        size = self.HEADER_LEN + 2+4+4 # acks + timeout + len(topics)
        for topic,parts in self._msets.iteritems():
            # topic name
            size += 2 + len(topic) + 4 # topic name + len(parts)
            # partition + mset size + len(mset)
            size += sum(4+4+len(mset) for mset in parts.itervalues())
        return size

    @property
    def API_KEY(self):
        return 0

    def add_messages(self, messages, topic_name, partition_num):
        self._msets[topic_name][partition_num].messages += messages

    def get_bytes(self):
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
        self.timeout = timeout
        self.min_bytes = min_bytes
        self._topics = defaultdict(dict)

    def add_fetch(self, topic_name, partition_num, offset, max_bytes=307200):
        self._topics[topic_name][partition_num] = (offset, max_bytes)

    def __len__(self):
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
        return 1

    def get_bytes(self):
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
    def __init__(self, max_offset, messages):
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
        fmt = [['S', ['ihqM']]]
        response,_ = self._unpack(fmt, buff, 0)
        errors = []
        self.topics = {}
        for (topic,partitions) in response[0]:
            for partition in partitions:
                message_set = MessageSet.decode(partition[3])
                self.topics[topic] = FetchPartitionResponse(
                    partition[2], message_set.messages
                )


# Offset API

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
        self._topics = defaultdict(dict)

    def __len__(self):
        # Header + replicaId + len(topics)
        size = self.HEADER_LEN + 4+4
        for topic,parts in self._topics.iteritems():
            # topic name + len(parts)
            size += 2+len(topic) + 4
            # partition + fetch offset + max bytes => for each partition
            size += (4+8+4) * len(parts)
        return size

    def add_topic(self, topic_name, partition_num, offsets_before, max_offsets=1):
        self._topics[topic_name][partition_num] = (offsets_before, max_offsets)

    @property
    def API_KEY(self):
        return 2

    def get_bytes(self):
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
        fmt = [ ['S', ['ih', ['q']]] ]
        response,_ = self._unpack(fmt, buff, 0)

        self.topics = {}
        for topic_name, partitions in response[0]:
            self.topics[topic_name] = {}
            for partition in partitions:
                self.topics[topic_name][partition[0]] = partition[2]
