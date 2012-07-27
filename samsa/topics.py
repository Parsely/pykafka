import logging
import random

from samsa.partitions import PartitionMap
from samsa.consumer import Consumer
from samsa.utils import attribute_repr


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
        """
        Returns a :class:`samsa.topics.Topic` for the given key.

        This is a proxy to :meth:`~TopicMap.get` for a more dict-like interface.
        """
        return self.get(key)

    def get(self, name):
        """
        Returns a :class:`samsa.topics.Topic` for this topic name, creating a
        new topic if one has not already been registered.
        """
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

    def subscribe(self, group):
        return Consumer(self.cluster, self, group)
