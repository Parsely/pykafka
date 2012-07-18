from samsa.utils.delayedconfig import DelayedConfiguration, requires_configuration


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

    def get(self, name):
        topic = self.__topics.get(name, None)
        if topic is None:
            topic = self.__topics[name] = Topic(self.cluster, name)
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


class PartitionMap(DelayedConfiguration):
    def __init__(self, cluster, topic):
        self.cluster = cluster
        self.topic = topic

    def _configure(self, event=None):
        raise NotImplementedError
