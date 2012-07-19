import collections
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
        node = '/brokers/topics/%s' % self.topic.name

        logger.info('Looking up brokers for %s...', self)

        # If the topic has never been posted to, it won't exist in ZooKeeper,
        # but every broker should be able to accept a write to the topic on
        # partition 0, since every node should handle one or more partitions
        # for each different topic.
        try:
            broker_ids = self.cluster.zookeeper.get_children(node, watch=self._configure)
        except NoNodeException:
            # To establish a watch on the topic node (which will be created on
            # the first write, and brokers will begin to disclose how many
            # partitions they are serving for this topic), we set a new watch.
            # If another node has published to this topic by the time we get
            # here (and the node exists), we can just start the configuration
            # process over again.
            if self.cluster.zookeeper.exists(node, watch=self._configure) is not None:
                logger.info('%s has been created, reconfiguring %s', node, self)
                return self._configure()

            logger.info('No brokers have been registered for %s, falling back to virtual partitions.', self)
            broker_ids = self.cluster.brokers.keys()

        alive = set()
        for broker_id in map(int, broker_ids):
            broker = self.cluster.brokers.get(broker_id)
            if broker not in self.__brokers:
                self.__brokers[broker] = PartitionSet(self.cluster, self.topic, broker)
            alive.add(broker)

        dead = set(self.__brokers.keys()) - alive
        for broker in dead:
            del self.__brokers[broker]

    @requires_configuration
    def __len__(self):
        return sum(map(len, self.__brokers.values()))

    @requires_configuration
    def __iter__(self):
        return itertools.chain.from_iterable(iter(value) for value in self.__brokers.values())


class PartitionSet(DelayedConfiguration):
    def __init__(self, cluster, topic, broker):
        self.cluster = cluster
        self.topic = topic
        self.broker = broker

        self.__count = None

    __repr__ = attribute_repr('topic', 'broker')

    def _configure(self, event=None):
        node = '/brokers/topics/%s/%s' % (self.topic.name, self.broker.id)

        # If the node does not exist, this means this broker has not gotten any
        # writes for this partition yet. We can assume that the broker is
        # handling at least one partition for this topic, and update when we
        # have more information by setting an exists watch on the node path.
        try:
            data, stat = self.cluster.zookeeper.get(node)
            count = int(data)
            logging.info('Found %s partitions for %s', count, self)
        except NoNodeException:
            if self.cluster.zookeeper.exists(node, watch=self._configure) is not None:
                return self._configure()
            count = 1
            logging.info('%s is not registered in ZooKeeper, falling back to %s virtual partition(s)',
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
