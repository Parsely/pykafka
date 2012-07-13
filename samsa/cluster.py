from samsa.brokers import BrokerMap
from samsa.topics import TopicMap


class Cluster(object):
    """
    A Kafka cluster.

    :ivar brokers: The :class:`samsa.brokers.BrokerMap` for this cluster.
    :ivar topics: The :class:`samsa.topics.TopicMap` for this cluster.

    :param zookeeper: A ZooKeeper client.
    :type zookeeper: :class:`kazoo.client.Client`
    """
    def __init__(self, zookeeper):
        self.zookeeper = zookeeper

        self.brokers = BrokerMap(self)
        self.topics = TopicMap(self)
