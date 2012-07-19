import collections
import functools
import itertools
import logging
import random

from zookeeper import NoNodeException

from samsa.utils import attribute_repr
from samsa.utils.delayedconfig import DelayedConfiguration, requires_configuration


logger = logging.getLogger(__name__)


class TopicMap(object):
    """
    Provides a dictionary-like interface to :class:`~samsa.topics.Topic`
    instances within a cluster.

    :param cluster: The cluster this topic mapping is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    """
    def __init__(self, cluster):
        self.cluster = cluster

        self.__topics = {}

    def __getitem__(self, key):
        return self.get(key)

    def get(self, name):
        topic = self.__topics.get(name, None)
        if topic is None:
            topic = self.__topics[name] = Topic(self.cluster, name)
            logger.info('Registered new topic: %s', topic)
        return topic


class Topic(object):
    """
    A topic within a Kafka cluster.

    :param cluster: The cluster that this topic is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    :param name: The name of this topic.
    """
    def __init__(self, cluster, name):
        self.cluster = cluster
        self.name = name
        self.partitions = PartitionMap(self.cluster, self)

    __repr__ = attribute_repr('name')

    def publish(self, data):
        """
        Publishes one or more messages to a random partition of this topic.
        """
        # TODO: This could/should be much more efficient.
        partition = random.choice(list(self.partitions))
        return partition.publish(data)


class PartitionMap(DelayedConfiguration):
    def __init__(self, cluster, topic):
        self.cluster = cluster
        self.topic = topic

        self.__brokers = {}

    __repr__ = attribute_repr('topic')

    def _configure(self, event=None):
        path = '/brokers/topics/%s' % self.topic.name
        logger.info('Looking up brokers for %s...', self)

        try:
            broker_ids = self.cluster.zookeeper.get_children(path, watch=self._configure)
        except NoNodeException:
            if self.cluster.zookeeper.exists(path, watch=self._configure) is not None:
                self._configure()
            broker_ids = []

        brokers = map(self.cluster.brokers.get, map(int, broker_ids))

        # Add any broker IDs that are not already present in the mapping.
        for broker in brokers:
            if broker not in self.__brokers:
                partitionset = PartitionSet(self.cluster, self.topic, broker)
                logging.info('Discovered new partition set: %s', partitionset)
                self.__brokers[broker] = partitionset

        # Remove any brokers that are no longer present in the mapping.
        dead = set(self.__brokers.keys()) - set(brokers)
        for broker in dead:
            logging.info('Removing broker %s from %s', broker, self)
            del self.__brokers[broker]

    @requires_configuration
    def __len__(self):
        return sum(map(len, self.__brokers.values()))

    @requires_configuration
    def __iter__(self):
        """
        Returns an iterator containing every known partition for this topic,
        including "virtual" partitions for brokers that are present in the
        cluster, but have not yet registered their topic/partitions with ZooKeeper.
        """
        partitionsets = itertools.chain.from_iterable(iter(value)
            for value in self.__brokers.values())

        # Each uninitialized broker should get a "virtual" partitionset.
        uninitialized_brokers = set(self.cluster.brokers.values()) - set(self.__brokers.keys())
        create_virtual_partitionset = functools.partial(PartitionSet,
            cluster=self.cluster, topic=self.topic, virtual=True)
        virtual_partitionsets = itertools.chain.from_iterable(
            iter(create_virtual_partitionset(broker=broker)) for broker in uninitialized_brokers)

        return itertools.chain(partitionsets, virtual_partitionsets)


class PartitionSet(DelayedConfiguration):
    def __init__(self, cluster, topic, broker, virtual=False):
        self.cluster = cluster
        self.topic = topic
        self.broker = broker
        self.virtual = virtual

        self.__count = None

    __repr__ = attribute_repr('topic', 'broker', 'virtual')

    def _configure(self, event=None):
        if self.virtual:
            count = 1
        else:
            node = '/brokers/topics/%s/%s' % (self.topic.name, self.broker.id)

            # If the node does not exist, this means this broker has not gotten any
            # writes for this partition yet. We can assume that the broker is
            # handling at least one partition for this topic, and update when we
            # have more information by setting an exists watch on the node path.
            try:
                data, stat = self.cluster.zookeeper.get(node)
                count = int(data)
                logger.info('Found %s partitions for %s', count, self)
            except NoNodeException:
                if self.cluster.zookeeper.exists(node, watch=self._configure) is not None:
                    return self._configure()
                count = 1
                logger.info('%s is not registered in ZooKeeper, falling back to %s virtual partition(s)',
                    self, count)

        self.__count = count

    def __iter__(self):
        for i in xrange(0, len(self)):
            yield Partition(self.cluster, self.topic, self.broker, i)

    @requires_configuration
    def __len__(self):
        return self.__count


class Partition(object):
    def __init__(self, cluster, topic, broker, number):
        self.cluster = cluster
        self.topic = topic
        self.broker = broker
        self.number = number

    __repr__ = attribute_repr('topic', 'broker', 'number')

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

        return self.broker.client.produce(self.topic.name, self.number, messages)
