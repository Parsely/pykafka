# - coding: utf-8 -
# TODO: Check all __repr__

import logging
import struct

from zlib import crc32

from samsa.utils import Serializable, attribute_repr, compression, unpack_from

logger = logging.getLogger(__name__)

class Cluster(object):
    """A Kafka cluster.

    :ivar brokers: The :class:`samsa.brokers.BrokerMap` for this cluster.
    :ivar topics: The :class:`samsa.topics.TopicMap` for this cluster.
    """
    def __init__(self, brokers, topics):
        # TODO: Saving so I remember to copy to new entry point
        #if not zookeeper.connected:
        #    raise Exception("Zookeeper must be connected before use.")
        #self.zookeeper = zookeeper
        #if not handler:
        #    handler = ThreadingHandler()
        #self.handler = handler
        self.brokers = brokers
        self.topics = topics


class Broker(object):
    """A Kafka broker.

    :param cluster: The cluster this broker is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    :param id_: Kafka broker ID
    """
    def __init__(self, id_, host, port):
        self._id = int(id_)
        self._host = host
        self._port = port
        self._client = None

    __repr__ = attribute_repr('id')

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
    def client(self):
        """The :class:`samsa.client.Client` object for this broker.

        Only one client is created per broker instance.
        """
        return self._client

    def connect(self):
        """Establish a connection to the Broker, creating a Client"""
        raise NotImplementedError()


class PartitionMetadata(object):
    """Metadata used to create a Partition when setting up a topic

    TODO: Note about why order of operations requires this
    """
    def __init__(self, id_, leader, replicas, isr):
        self.id = id_
        self.leader = leader
        self.replicas = replicas
        self.isr = isr


class Partition(object):
    def __init__(self, topic, id_, leader, replicas, isr):
        self.id = id_
        self.leader = leader
        self.replicas = replicas
        self.isr = isr
        self.topic = topic

    __repr__ = attribute_repr('topic', 'broker', 'number')

    def earliest_offset(self):
        return self.broker.client.offsets(
            self.topic.name, self.number, OFFSET_EARLIEST, 1
        )[0]

    def latest_offset(self):
        return self.broker.client.offsets(
            self.topic.name, self.number, OFFSET_LATEST, 1
        )[0]

    def publish(self, data):
        """
        Publishes one or more messages to this partition.
        """
        if isinstance(data, basestring):
            messages = [data]
        elif isinstance(data, collections.Sequence):
            messages = data
        else:
            raise TypeError

        return self.broker.client.produce(self.topic.name, self.number,
            messages)

    def fetch(self, offset, size):
        return self.broker.client.fetch(
            self.topic.name, self.number, offset, size
        )

    def __hash__(self):
        return hash((self.topic, self.broker.id, self.number))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self == other


class Topic(object):
    """A topic within a Kafka cluster.

    :param cluster: The cluster that this topic is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    :param name: The name of this topic.
    :param partitioner: callable that takes two arguments, ``partitions`` and
        ``key`` and returns a single :class:`~samsa.partitions.Partition`
        instance to publish the message to.
    :type partitioner: any callable type
    """
    def __init__(self, name, partition_metas, brokers):
        self.name = name
        self.partitions = {
            pm.id: Partition(self, pm.id, brokers[pm.leader],
                             [brokers[id_] for id_ in pm.replicas],
                             [brokers[id_] for id_ in pm.isr])
            for pm in partition_metas
        }

    __repr__ = attribute_repr('name')

    def latest_offsets(self):
        return [(p.broker.id, p.latest_offset())
                for p
                in self.partitions]

    def publish(self, data, key=None):
        """
        Publishes one or more messages to a random partition of this topic.

        :param data: message(s) to be sent to the broker.
        :type data: ``str`` or sequence of ``str``.
        :param key: a key to be used for semantic partitioning
        :type key: implementation-specific
        """
        if len(self.partitions) < 1:
            raise NoAvailablePartitionsError('No partitions are available to '
                'accept a write for this message. (Is your Kafka broker '
                'running?)')
        partition = self.partitioner(self.partitions, key)
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


class Message(Serializable):
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
    """
    MAGIC = 0

    def __init__(self, value, partition_key=None, compression=compression.NONE, offset=-1):
        self.compression = compression
        self.partition_key = partition_key
        self.value = value
        self.offset = offset

    def __len__(self):
        size = 4+1+1+4+4+len(self.value)
        if self.partition_key is not None:
            size += len(self.partition_key)
        return size

    @classmethod
    def decode(self, buff, msg_offset=-1):
        fmt = 'iBBYY'
        response = unpack_from(fmt, buff, 0)
        crc,_,attr,key,val = response[0]
        crc_check = crc32(buff[4:])
        # TODO: Handle CRC failure
        return Message(val, partition_key=key, compression=attr, offset=msg_offset)

    def pack_into(self, buff, offset):
        """Serialize and write to ``buff`` starting at offset ``offset``.

        Intentionally follows the pattern of ``struct.pack_into``

        :param buff: The buffer to write into
        :param offset: The offset to start the write at
        """
        if self.partition_key is None:
            fmt = '!BBii%ds' % len(self.value)
            args = (self.MAGIC, self.compression, -1,
                    len(self.value), self.value)
        else:
            fmt = '!BBi%dsi%ds' % (len(self.partition_key), len(self.value))
            args = (self.MAGIC, self.compression,
                    len(self.partition_key), self.partition_key,
                    len(self.value), self.value)
        struct.pack_into(fmt, buff, offset+4, *args)
        fmt_size = struct.calcsize(fmt)
        crc = crc32(buffer(buff[offset+4:offset+4+fmt_size]))
        struct.pack_into('!i', buff, offset, crc)
