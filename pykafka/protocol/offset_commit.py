# - coding: utf-8 -
import struct
from collections import namedtuple, defaultdict

from .base import Request, Response
from ..utils import struct_helpers
from ..utils.compat import iteritems


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
    API_VERSION = 0
    API_KEY = 9

    @classmethod
    def get_versions(cls):
        return {0: OffsetFetchRequest, 1: OffsetFetchRequestV1, 2: OffsetFetchRequestV2}

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

    def _reqs_len(self):
        return len(self._reqs)

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
        self._write_header(output, api_version=self.API_VERSION)
        offset = self.HEADER_LEN
        fmt = '!h%dsi' % len(self.consumer_group)
        struct.pack_into(fmt, output, offset,
                         len(self.consumer_group), self.consumer_group,
                         self._reqs_len())
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


class OffsetFetchRequestV1(OffsetFetchRequest):
    API_VERSION = 1


class OffsetFetchRequestV2(OffsetFetchRequestV1):
    API_VERSION = 2

    def _reqs_len(self):
        # v2 allows a null array to select all topics
        return len(self._reqs) or -1


OffsetFetchPartitionResponse = namedtuple(
    'OffsetFetchPartitionResponse',
    ['offset', 'metadata', 'err']
)


class OffsetFetchResponse(Response):
    """An offset fetch response v0
    Specification::
    OffsetFetch Response (Version: 0) => [responses]
        responses => topic [partition_responses]
            topic => STRING
            partition_responses => partition offset metadata error_code
                partition => INT32
                offset => INT64
                metadata => NULLABLE_STRING
                error_code => INT16
    """
    API_VERSION = 0
    API_KEY = 9

    @classmethod
    def get_versions(cls):
        return {0: OffsetFetchResponse, 1: OffsetFetchResponseV1, 2: OffsetFetchResponseV2}

    def __init__(self, buff):
        """Deserialize into a new Response
        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = '[S [iqSh ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        self._populate_partition_responses(response)

    def _populate_partition_responses(self, partition_responses):
        self.topics = {}
        for topic_name, partitions in partition_responses:
            self.topics[topic_name] = {}
            for partition in partitions:
                pres = OffsetFetchPartitionResponse(partition[1],
                                                    partition[2],
                                                    partition[3])
                self.topics[topic_name][partition[0]] = pres


class OffsetFetchResponseV1(OffsetFetchResponse):
    """An offset fetch response v1 (all the same as v0)
    Specification::
    OffsetFetch Response (Version: 1) => [responses]
        responses => topic [partition_responses]
            topic => STRING
            partition_responses => partition offset metadata error_code
                partition => INT32
                offset => INT64
                metadata => NULLABLE_STRING
                error_code => INT16
    """
    API_VERSION = 1


class OffsetFetchResponseV2(OffsetFetchResponseV1):
    """An offset fetch response v2
    Specification::
    OffsetFetch Response (Version: 2) => [responses] error_code
        responses => topic [partition_responses]
            topic => STRING
            partition_responses => partition offset metadata error_code
                partition => INT32
                offset => INT64
                metadata => NULLABLE_STRING
                error_code => INT16
        error_code => INT16 (new since v1)
    """
    API_VERSION = 2

    def __init__(self, buff):
        fmt = '[S [iqSh ] ] h'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        partition_responses, self.err = response
        self._populate_partition_responses(partition_responses)
