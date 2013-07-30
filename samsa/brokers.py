__license__ = """
Copyright 2012 DISQUS
Copyright 2013 Parse.ly, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging

from kazoo.recipe.watchers import DataWatch, ChildrenWatch
from kazoo.exceptions import NoNodeException

from samsa.client import Client
from samsa.exceptions import ImproperlyConfiguredError
from samsa.utils import attribute_repr


logger = logging.getLogger(__name__)


class BrokerMap(object):
    """
    Represents the topology of all brokers within a Kafka cluster.
    """
    def __init__(self, cluster):
        self.cluster = cluster

        # The internal cache of all brokers available within the cluster.
        self.__brokers = {}

        self._node_path = '/brokers/ids'
        try:
            self._broker_watch = ChildrenWatch(
                self.cluster.zookeeper,
                self._node_path, self._configure
            )
        except NoNodeException:
            raise ImproperlyConfiguredError(
                'The path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?' %
                self._node_path)

    def _configure(self, broker_ids):
        """
        Configures the broker mapping.
        """
        # TODO: If this fails, would it make more sense to open a watch on the
        # key, and just return that there are no brokers that are alive, to
        # avoid any race conditions between cluster/application startup?
        logger.info('Refreshing broker configuration from %s...',
            self.cluster.zookeeper)

        alive = set()
        for broker_id in map(int, broker_ids):
            if broker_id not in self.__brokers:
                broker = Broker(self.cluster, id_=broker_id)
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

    def __len__(self):
        """
        Returns the number of all active brokers.
        """
        return len(self.__brokers)

    def __iter__(self):
        """
        Returns an iterator containing all of the broker IDs within the
        cluster.
        """
        return iter(self.__brokers)

    def __getitem__(self, id_):
        """
        Returns a broker by it's broker ID.
        """
        return self.get(id_)

    def get(self, id_):
        """
        Returns a broker by it's broker ID.
        """
        return self.__brokers[id_]

    def keys(self):
        """
        Returns a list of all broker IDs within the cluster.
        """
        return self.__brokers.keys()

    def values(self):
        """
        Returns a list of every :class:`~samsa.brokers.Broker` within the
        cluster.
        """
        return self.__brokers.values()

    def items(self):
        """
        Returns a list of 2-tuples of the format ``(id, broker)``, where
        ``broker`` is a :class:`~samsa.brokers.Broker` instance.
        """
        return self.__brokers.items()


class Broker(object):
    """
    A Kafka broker.

    :param cluster: The cluster this broker is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    :param id_: Kafka broker ID
    """
    def __init__(self, cluster, id_):
        self.cluster = cluster
        self.id = int(id_)

        self.__host = None
        self.__port = None

        self.is_dead = False

        self._node_path = '/brokers/ids/%s' % self.id
        self._config_watcher = DataWatch(
            self.cluster.zookeeper,
            self._node_path, self._configure
        )

    __repr__ = attribute_repr('id')

    def _configure(self, data, stat):
        """
        Configures a broker based on it's state in ZooKeeper.
        """
        logger.info('Retrieved broker data for %s...', self)
        if data is None:
            logger.info('Broker data field was empty. Assuming it was dead.')
            self.__host = self.__port = None
        else:
            creator, self.__host, port = data.split(':')
            self.__port = int(port)

    @property
    def host(self):
        """
        The host that the broker is available at.
        """
        return self.__host

    @property
    def port(self):
        """
        The port that the broker is available at.
        """
        return self.__port

    @property
    def client(self):
        """
        The :class:`samsa.client.Client` object for this broker.

        Only one client is created per broker instance.
        """
        try:
            return self.__client
        except AttributeError:
            self.__client = Client(self.host, self.cluster.handler,
                                   port=self.port)
            return self.__client
