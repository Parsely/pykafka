# - coding: utf-8 -

# TODO: Check all __repr__
# TODO: __slots__ where appropriate
# TODO: Use weak refs to avoid reference cycles?

import logging
import itertools
import struct

from collections import defaultdict
from zlib import crc32

from samsa.connection import BrokerConnection
from samsa.handlers import RequestHandler
from samsa.partitioners import random_partitioner
from samsa.utils import Serializable, attribute_repr, compression
from samsa.utils.protocol import (
    FetchRequest, FetchResponse, MetadataRequest, MetadataResponse,
    OFFSET_EARLIEST, OFFSET_LATEST, OffsetRequest, OffsetResponse,
    PartitionFetchRequest, PartitionOffsetRequest,
    ProduceRequest, ProduceResponse, Message, MessageSet
)

logger = logging.getLogger(__name__)

class Broker(object):
    """A Kafka broker.

    :param cluster: The cluster this broker is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    :param id_: Kafka broker ID
    """
    def __init__(self, id_, host, port, handler, timeout):
        self._connected = False
        self._id = int(id_)
        self._host = host
        self._port = port
        self._handler = handler
        self._reqhandler = None
        self._timeout = timeout
        self.client = None

    __repr__ = attribute_repr('id')

    @property
    def connected(self):
        return self._connected

    @property
    def id(self):
        """The broker's ID within the Kafka cluster."""
        return self._id

    @property
    def host(self):
        """The host where the broker is available."""
        return self._host

    @property
    def port(self):
        """The port where the broker is available."""
        return self._port

    @property
    def handler(self):
        """The :class:`samsa.handlers.RequestHandler` for this broker.

        Only one handler is created per broker instance.
        """
        return self._reqhandler

    def connect(self):
        """Establish a connection to the Broker, creating a Client"""
        conn = BrokerConnection(self.host, self.port)
        conn.connect(self._timeout)
        self._reqhandler = RequestHandler(self._handler, conn)
        self._reqhandler.start()
        self._connected = True

    def fetch_messages(self, partition_requests, timeout=30000, min_bytes=1024):
        if not self.connected:
            self.connect()
        future = self.handler.request(FetchRequest(
            partition_requests=partition_requests,
            timeout=10000,
            min_bytes=1,
        ))
        return future.get(FetchResponse)

    def produce_messages(self,
                         topic_name,
                         partition_id,
                         messages,
                         required_acks=1,
                         timeout=10000):
        if not self.connected:
            self.connect()
        req = ProduceRequest(required_acks=required_acks, timeout=timeout)
        req.add_messages(messages, topic_name, partition_id)
        future = self.handler.request(req)
        return future.get(ProduceResponse)

    def request_offsets(self, partition_requests):
        """Request offset information for a set of topic/partitions"""
        if not self.connected:
            self.connect()
        future = self.handler.request(OffsetRequest(partition_requests))
        return future.get(OffsetResponse)

    def request_metadata(self, topics=[]):
        if not self.connected:
            self.connect()
        future = self.handler.request(MetadataRequest(topics=topics))
        return future.get(MetadataResponse)


class Partition(object):
    def __init__(self, topic, id_, leader, replicas, isr):
        self.id = id_
        self.leader = leader
        self.replicas = replicas
        self.isr = isr
        self.topic = topic

    __repr__ = attribute_repr('topic', 'broker', 'number')

    def fetch_offsets(self, offsets_before, max_offsets=1):
        request = PartitionOffsetRequest(
            self.topic.name, self.id, offsets_before, max_offsets
        )
        res = self.leader.request_offsets([request])
        return res.topics[self.topic.name]

    def latest_offset(self):
        """Get the latest offset for this partition."""
        return self.fetch_offsets(OFFSET_LATEST)

    def earliest_offset(self):
        """Get the earliest offset for this partition."""
        return self.fetch_offsets(OFFSET_EARLIEST)

    def publish(self, data, partition_key=None):
        """
        Publishes one or more messages to this partition.
        """
        if isinstance(data, basestring):
            messages = [Message(data, partition_key=partition_key)]
        elif isinstance(data, collections.Sequence):
            messages = [Message(d, partition_key=partition_key)
                        for d in data]
        else:
            raise TypeError('Unable to publish data of type %s' % type(data))

        # TODO: Compression here?
        return self.leader.produce_messages(self.topic.name, self.id, messages)

    def fetch_messages(self, offset, max_bytes=307200):
        req = PartitionFetchRequest(self.topic.name, self.id, offset, max_bytes)
        res = self.leader.fetch_messages([req])
        return list(itertools.chain.from_iterable(
            t.messages
            for t in res.topics.itervalues()
        ))

    def __hash__(self):
        return hash((self.topic, self.broker.id, self.number))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self == other


class Topic(object):
    """A topic within a Kafka cluster"""
    def __init__(self, name):
        self.client = None
        self.name = name
        self.partitions = {}

    __repr__ = attribute_repr('name')

    def fetch_offsets(self, offsets_before, max_offsets=1):
        requests = defaultdict(list) # one request for each broker
        for part in self.partitions.itervalues():
            requests[part.leader].append(PartitionOffsetRequest(
                self.name, part.id, offsets_before, max_offsets
            ))
        output = {}
        for broker,reqs in requests.iteritems():
            res = broker.request_offsets(reqs)
            output.update(res.topics[self.name])
        return output

    def latest_offsets(self):
        """Get the latest offset for each partition of this topic."""
        return self.fetch_offsets(OFFSET_LATEST)

    def earliest_offsets(self):
        """Get the earliest offset for each partition of this topic."""
        return self.fetch_offsets(OFFSET_EARLIEST)

    def publish(self, data, partitioner=random_partitioner, partition_key=None):
        """Publish one or more messages to a partition of this topic.

        :param data: message(s) to be sent to the broker.
        :type data: ``str`` or sequence of ``str``.
        :param partitioner: callable that takes two arguments, ``partitions``
            and ``key`` and returns a single :class:`~samsa.common.Partition`
            instance to publish the message to.
        :type partitioner: any callable
        :param partition_key: a key to be used for semantic partitioning
        :type partition_key: implementation-specific
        """
        if len(self.partitions) < 1:
            raise NoAvailablePartitionsError('No partitions are available to '
                'accept a write for this message. (Is your Kafka broker '
                'running?)')
        partition = partitioner(self.partitions, partition_key)
        return partition.publish(data)

    def subscribe(self,
                  group,
                  backoff_increment=1,
                  connect_retries=4,
                  fetch_size=307200,
                  offset_reset='nearest',
                  rebalance_retries=4,
                  ):
        """
        Returns a new consumer that can be used for reading from this topic.

        `backoff_increment` is used to progressively back off asking a partition
        for messages when there aren't any ready. Incrementally increases wait
        time in seconds.

        `offset_reset` is used to determine where to reset a partition's offset
        in the event of an OffsetOutOfRangeError. Valid values are:

        "earliest": Go to the earliest message in the partition
        "latest": Go to the latest message in the partition
        "nearest": If requested offset is before the earliest, go there,
                   otherwise, go to the latest message in the partition.

        `rebalance_retries` and `connect_retries` affect the number of times
        to try acquiring partitions before giving up.

        When samsa restarts, there can be a bit of lag before
        Zookeeper realizes the old client is dead and releases the partitions
        it was consuming. Setting this means samsa will wait a bit and try to
        acquire partitions again before throwing an error. In the case of
        rebalancing, sometimes it takes a bit for a consumer to release the
        partition they're reading, and this helps account for that.

        :param group: The consumer group to join.
        :param backoff_increment: How fast to incrementally backoff when a
                                  partition has no messages to read.
        :param connect_retries: Retries before giving up on connecting
        :param fetch_size: Default fetch size (in bytes) to get from Kafka
        :param offset_reset: Where to reset when an OffsetOutOfRange happens
        :param rebalance_retries: Retries before giving up on rebalance
        :rtype: :class:`samsa.consumer.consumer.Consumer`
        """
        return Consumer(self.cluster,
                        self,
                        group,
                        backoff_increment=backoff_increment,
                        connect_retries=connect_retries,
                        fetch_size=fetch_size,
                        offset_reset=offset_reset,
                        rebalance_retries=rebalance_retries)
