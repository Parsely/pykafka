class BrokerMap(object):
    """
    Represents the topology of all brokers within a Kafka cluster.
    """
    def __init__(self, cluster):
        self.cluster = cluster


class Broker(object):
    """
    A Kafka broker.

    :param cluster: The cluster this broker is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    """
    def __init__(self, cluster):
        self.cluster = cluster
