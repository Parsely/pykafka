__license__ = """
Copyright 2012 DISQUS
Copyright 2013,2014 Parse.ly, Inc.

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

import itertools
import json
import logging

from kazoo.exceptions import NoNodeException
from samsa import handlers
from samsa.common import Broker, Topic, Partition
from samsa.connection import BrokerConnection
from samsa.exceptions import ImproperlyConfiguredError
from samsa.utils.protocol import MetadataRequest, MetadataResponse
from zlib import crc32


logger = logging.getLogger(__name__)

class SamsaClient(object):
    """Main entry point for a Kafka cluster

    Notes:
        * Reconfiguring at any time is hard to coordinate. Updating
          the cluster should only happen on user actions?

    :ivar brokers: The :class:`samsa.common.Broker` map for this cluster.
    :ivar topics: The :class:`samsa.common.Topic` map for this cluster.
    """
    def __init__(self, zookeeper, handler=None, timeout=30):
        """Create a connection to a Kafka cluster

        :param zookeeper: A zookeeper client.
        :type zookeeper: :class:`kazoo.client.Client`
        :param handler: Async handler.
        :type handler: :class:`samsa.handlers.Handler`
        """
        if not zookeeper.connected:
            raise Exception("Zookeeper must be connected before use.")
        self.zookeeper = zookeeper
        self.handler = handler or handlers.ThreadingHandler()
        self.topics = self.brokers = None
        self._timeout = timeout
        self.brokers = {}
        self.topics = {}
        self.update_cluster()

    def _discover_brokers(self):
        """Get the list of brokers from Zookeeper.

        :returns: list of `(host, port)` for each broker.
        """
        id_path = '/brokers/ids'
        output = []
        try:
            broker_ids = self.zookeeper.get_children(id_path)
            for id_ in broker_ids:
                data = json.loads(
                    self.zookeeper.get('%s/%s' % (id_path, id_))[0]
                )
                output.append(Broker(id_, data['host'], data['port'],
                                     self.handler, self._timeout))
        except NoNodeException:
            raise ImproperlyConfiguredError(
                'The path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?' %
                id_path)
        return output

    def _get_metadata(self):
        """Get fresh cluster metadata from a broker"""
        if self.brokers:
            brokers = self.brokers.values()
        else:
            brokers = self._discover_brokers()
        for broker in brokers:
            try:
                return broker.request_metadata()
            except Exception, e:
                logger.warning('Unable to connect to broker %s:%s',
                               broker.host, broker.port)
        raise Exception('Unable to connect to a broker to fetch metadata.')

    def _update_brokers(self, broker_metadata):
        # Remove old brokers
        removed = set(self.brokers.keys()) - set(broker_metadata.keys())
        for id_ in removed:
            logger.info('Removing broker %s', self.brokers[id_])
            self.brokers.pop(id_)
        # Add/update current brokers
        for id_,meta in broker_metadata.iteritems():
            if id_ not in self.brokers:
                self.brokers[id_] = Broker(meta.id, meta.host, meta.port,
                                           self.handler, self._timeout)
                logger.info('Adding new broker %s', self.brokers[id_])
            else:
                broker = self.brokers[id_]
                if meta.host == broker.host and meta.port == broker.port:
                    continue # no changes
                if broker.connected:
                    broker.disconnect()
                logger.info('Updating broker %s', broker)
                broker.host = meta.host
                broker.port = meta.port
                logger.info('Updated broker to %s', broker)

    def _update_topics(self, topic_metadata):
        # Remove old topics
        removed = set(self.topics.keys()) - set(topic_metadata.keys())
        for name in removed:
            logger.info('Removing topic %s', self.topic[name])
            self.topics.pop(name)
        # Add/update partition information
        for name,meta in topic_metadata.iteritems():
            if name not in self.topics:
                self.topics[name] = Topic(name)
                logger.info('Adding topic %s', self.topics[name])
            # Partitions always need to be updated, even on add
            self._update_partitions(self.topics[name], meta.partitions)

    def _update_partitions(self, topic, partition_metadata):
        # Remove old partitions
        removed = set(topic.partitions.keys()) - set(partition_metadata.keys())
        for id_ in removed:
            logger.info('Removing partiton %s', topic.partitons[id_])
            self.brokers.pop(id_)
        # Make sure any brokers referenced are known
        all_brokers = itertools.chain.from_iterable(
            [meta.id,]+meta.isr+meta.replicas
            for meta in partition_metadata.itervalues()
        )
        if any(b not in self.brokers for b in all_brokers):
            raise Exception('TODO: Type this exception')
        # Add/update current partitions
        for id_,meta in partition_metadata.iteritems():
            if meta.id not in topic.partitions:
                topic.partitions[meta.id] = Partition(
                    topic, meta.id, self.brokers[meta.leader],
                    [self.brokers[b] for b in meta.replicas],
                    [self.brokers[b] for b in meta.isr]
                )
                logger.info('Adding partition %s', topic.partitions[meta.id])
            partition = topic.partitions[id_]
            # Check leader
            if meta.leader != partition.leader.id:
                logger.info('Updating leader for %s', partition)
                partition.leader = self.brokers[meta.leader]
            # Check replica and In-Sync-Replicas lists
            if sorted(r.id for r in partition.replicas) != sorted(meta.replicas):
                logger.info('Updating replicas list for %s', partition)
                partition.replicas = [self.brokers[b] for b in meta.replicas]
            if sorted(i.id for i in partition.isr) != sorted(meta.isr):
                logger.info('Updating in sync replicas list for %s', partition)
                partition.isr = [self.brokers[b] for b in meta.isr]


    def update_cluster(self):
        """Update known brokers and topics

        We actually want to update this all in place, rather than paving over
        previously known topics/partitions with new information. We do this
        so that a topic can call ``self.client.update()`` to find new partition
        leaders. If we replaced ``self.topics`` with a new dict, then that
        partition info wouldn't be updated and that topic would be orphaned.
        """
        metadata = self._get_metadata()
        self._update_brokers(metadata.brokers)
        self._update_topics(metadata.topics)
