# - coding: utf-8 -
import struct
from collections import namedtuple

from .base import Request, Response
from ..utils import struct_helpers


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
