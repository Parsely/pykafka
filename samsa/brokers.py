from zookeeper import NoNodeException

from samsa.exceptions import ImproperlyConfigured
from samsa.utils.delayedconfig import DelayedConfiguration, requires_configuration


class BrokerMap(DelayedConfiguration):
    """
    Represents the topology of all brokers within a Kafka cluster.
    """
    def __init__(self, cluster):
        self.cluster = cluster

        # The internal cache of all brokers available within the cluster.
        self.__brokers = {}

    def _configure(self, event=None):
        """
        Configures the broker mapping and monitors for state changes, updating
        the internal mapping when the cluster topology changes.
        """
        # TODO: If this fails, would it make more sense to open a watch on the
        # key, and just return that there are no brokers that are alive, to
        # avoid any race conditions between cluster/application startup?
        path = '/brokers/ids'
        try:
            broker_ids = self.cluster.zookeeper.get_children(path, watch=self._configure)
        except NoNodeException:
            raise ImproperlyConfigured('The path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?' % path)

        alive = set()
        for broker_id in broker_ids:
            broker = Broker(self.cluster, id=broker_id)
            self.__brokers[broker.id] = broker
            alive.add(broker.id)

        dead = set(self.__brokers.keys()) - alive
        for broker_id in dead:
            broker = self.__brokers[broker_id]
            broker.is_dead = True
            del self.__brokers[broker.id]

    # TODO: Add all proxies to appropriate `dict` interface.

    @requires_configuration
    def __len__(self):
        return len(self.__brokers)

    @requires_configuration
    def get(self, id):
        return self.__brokers[id]

    @requires_configuration
    def keys(self):
        return self.__brokers.keys()

    @requires_configuration
    def values(self):
        return self.__brokers.values()


class Broker(DelayedConfiguration):
    """
    A Kafka broker.

    :param cluster: The cluster this broker is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    :param id: Kafka broker ID
    """
    def __init__(self, cluster, id):
        self.cluster = cluster
        self.id = int(id)

        self.__host = None
        self.__port = None

        self.is_dead = False

    def _configure(self, event=None):
        node = '/brokers/ids/%s' % self.id
        data, stat = self.cluster.zookeeper.get(node, watch=self._configure)
        creator, self.__host, port = data.split(':')
        self.__port = int(port)

    @property
    @requires_configuration
    def host(self):
        return self.__host

    @property
    @requires_configuration
    def port(self):
        return self.__port
