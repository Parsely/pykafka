# - coding: utf-8 -
import struct
from collections import namedtuple, defaultdict

from .base import Request, Response
from ..utils import struct_helpers
from ..utils.compat import iteritems


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


class ListOffsetRequest(Request):
    """An offset request
    Specification::
        ListOffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
          ReplicaId => int32
          TopicName => string
          Partition => int32
          Time => int64
          MaxNumberOfOffsets => int32
    """
    API_VERSION = 0
    API_KEY = 2

    @classmethod
    def get_versions(cls):
        # XXX use ListOffsetRequestV1 after 0.10 message format is supported
        return {0: ListOffsetRequest, 1: ListOffsetRequest}

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
        self._write_header(output, api_version=self.API_VERSION)
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


class ListOffsetRequestV1(ListOffsetRequest):
    """
    Specification::
        ListOffsetRequest => ReplicaId [TopicName [Partition Time]]
          ReplicaId => int32
          TopicName => string
          Partition => int32
          Time => int64
    """
    API_VERSION = 1

    def __init__(self, partition_requests):
        """Create a new offset request"""
        self._reqs = defaultdict(dict)
        for t in partition_requests:
            self._reqs[t.topic_name][t.partition_id] = t.offsets_before

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + replicaId + len(topics)
        size = self.HEADER_LEN + 4 + 4
        for topic, parts in iteritems(self._reqs):
            # topic name + len(parts)
            size += 2 + len(topic) + 4
            # partition + fetch offset => for each partition
            size += (4 + 8) * len(parts)
        return size

    def get_bytes(self):
        """Serialize the message
        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output, api_version=self.API_VERSION)
        offset = self.HEADER_LEN
        struct.pack_into('!ii', output, offset, -1, len(self._reqs))
        offset += 8
        for topic_name, partitions in iteritems(self._reqs):
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name),
                             topic_name, len(partitions))
            offset += struct.calcsize(fmt)
            for pnum, offsets_before in iteritems(partitions):
                struct.pack_into('!iq', output, offset, pnum, offsets_before)
                offset += 12
        return output


OffsetPartitionResponse = namedtuple(
    'OffsetPartitionResponse',
    ['offset', 'err']
)


OffsetPartitionResponseV1 = namedtuple(
    'OffsetPartitionResponseV1',
    ['offset', 'timestamp', 'err']
)


class ListOffsetResponse(Response):
    """An offset response
    Specification::
        ListOffsetResponse => [TopicName [PartitionOffsets]]
          PartitionOffsets => Partition ErrorCode [Offset]
          Partition => int32
          ErrorCode => int16
          Offset => int64
    """
    API_VERSION = 0
    API_KEY = 2

    @classmethod
    def get_versions(cls):
        # XXX use ListOffsetResponseV1 after 0.10 message format is supported
        return {0: ListOffsetResponse, 1: ListOffsetResponse}

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


class ListOffsetResponseV1(ListOffsetResponse):
    """
    Specification::
        ListOffsetResponse => [TopicName [PartitionOffsets]]
          PartitionOffsets => Partition ErrorCode Timestamp [Offset]
          Partition => int32
          ErrorCode => int16
          Timestamp => int64
          Offset => int64
    """
    API_VERSION = 1

    def __init__(self, buff):
        """Deserialize into a new Response
        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[S [ihq [q] ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.topics = {}
        for topic_name, partitions in response:
            self.topics[topic_name] = {}
            for partition in partitions:
                self.topics[topic_name][partition[0]] = OffsetPartitionResponseV1(
                    partition[3], partition[2], partition[1])
