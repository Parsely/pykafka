import logging

from zookeeper import NoNodeException

from samsa.client import Client
from samsa.exceptions import ImproperlyConfigured
from samsa.utils import attribute_repr
from samsa.utils.delayedconfig import DelayedConfiguration, requires_configuration


logger = logging.getLogger(__name__)


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
        logger.info('Refreshing broker configuration from %s...', self.cluster.zookeeper)
        try:
            broker_ids = self.cluster.zookeeper.get_children(path, watch=self._configure)
        except NoNodeException:
            raise ImproperlyConfigured('The path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?' % path)

        alive = set()
        for broker_id in map(int, broker_ids):
            if broker_id not in self.__brokers:
                broker = Broker(self.cluster, id=broker_id)
                logger.info('Adding new broker to %s: %s', self, broker)
                self.__brokers[broker.id] = broker
            alive.add(broker_id)

        dead = set(self.__brokers.keys()) - alive
        for broker_id in dead:
            broker = self.__brokers[broker_id]
            logger.info('Removing dead broker from %s: %s', self, broker)
            broker.is_dead = True
            del self.__brokers[broker.id]

    # TODO: Add all proxies to appropriate `dict` interface.

    @requires_configuration
    def __len__(self):
        return len(self.__brokers)

    @requires_configuration
    def __iter__(self):
        return iter(self.__brokers)

    @requires_configuration
    def __getitem__(self, key):
        return self.__brokers[key]

    @requires_configuration
    def get(self, id):
        return self.__brokers[id]

    @requires_configuration
    def keys(self):
        return self.__brokers.keys()

    @requires_configuration
    def values(self):
        return self.__brokers.values()

    @requires_configuration
    def items(self):
        return self.__brokers.items()


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

    __repr__ = attribute_repr('id')

    def _configure(self, event=None):
        logger.info('Fetching broker data for %s...', self)
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

    @property
    def client(self):
        try:
            return self.__client
        except AttributeError:
            self.__client = Client(self.host, self.port)
            return self.__client
