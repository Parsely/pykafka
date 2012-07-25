import logging
import itertools
import socket

from collections import namedtuple
from kazoo.exceptions import NodeExistsException, NoNodeException
from uuid import uuid4

from samsa.exceptions import ImproperlyConfigured
from samsa.utils.delayedconfig import DelayedConfiguration, requires_configuration

logger = logging.getLogger(__name__)

class PartitionName(namedtuple('PartitionName', ['broker_id', 'partition_id'])):

    @staticmethod
    def from_str(s):
        broker_id, partition_id = s.split('-')
        return PartitionName(broker_id, partition_id)

    @staticmethod
    def from_partition(p):
        return PartitionName(p.broker.id, p.number)

    def to_str(self):
        return "%s-%s" % (self.broker_id, self.partition_id)

    def __str__(self):
        return self.to_str()


class PartitionOwnerRegistry(DelayedConfiguration):
    """
    Manages the Partition Owner Registry for a particular Consumer.
    """

    def __init__(self, consumer, cluster, topic, group):
        self.consumer_id = str(consumer.id)
        self.cluster = cluster
        self.path = '/consumers/%s/owners/%s' % (group, topic.name)
        self.cluster.zookeeper.ensure_path(self.path)

    def _configure(self, event=None):
        self._partitions = set([])

        zk = self.cluster.zookeeper
        partitions = zk.get_children(self.path, watch=self._configure)

        for name in partitions:
            p = PartitionName.from_str(name)
            value, _ = zk.get(self._path_from_partition(p))
            if value == self.consumer_id:
                self._partitions.add(p)

    @requires_configuration
    def get(self):
        return self._partitions

    @requires_configuration
    def remove(self, partitions):
        print "PartitionOwnerRegistry.remove(%s)" % partitions
        for p in partitions:
            assert p in self._partitions
            self.cluster.zookeeper.delete(self._path_from_partition(p))
            self._partitions.remove(p)

    @requires_configuration
    def add(self, partitions):
        print "PartitionOwnerRegistry.add(%s)" % partitions
        for p in partitions:
            self.cluster.zookeeper.create(
                self._path_from_partition(p), self.consumer_id, ephemeral=True
            )
            self._partitions.add(p)

    def _path_from_partition(self, p):
        return "%s/%s" % (self.path, p)


class Consumer(object):

    MAX_RETRIES = 5

    def __init__(self, cluster, topic, group):
        self.cluster = cluster
        self.topic = topic
        self.group = group
        self.id = "%s:%s" % (socket.gethostname(), uuid4())

        self.id_path = '/consumers/%s/ids' % self.group
        self.cluster.zookeeper.ensure_path(self.id_path)

        self.partition_owner_registry = PartitionOwnerRegistry(
            self, cluster, topic, group)

        path = '%s/%s' % (self.id_path, self.id)
        self.cluster.zookeeper.create(path, self.topic.name, ephemeral=True)

        self._rebalance()

    def _rebalance(self, event=None):
        """
        Joins a consumer group and claims partitions.
        """

        #print "_rebalance(%s)" % self.id
        logger.info('Rebalancing consumer %s for topic %s.' % (
            self.id, self.topic.name)
        )

        zk = self.cluster.zookeeper
        broker_path = '/brokers/ids'
        try:
            zk.get_children(broker_path, watch=self._rebalance)
        except NoNodeException:
            raise ImproperlyConfigured('The broker_path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?' % broker_path)

        # 3. all consumers in the same group as Ci that consume topic T
        consumer_ids = zk.get_children(self.id_path, watch=self._rebalance)
        participants = []
        for id_ in consumer_ids:
            topic, stat = zk.get("%s/%s" % (self.id_path, id_))
            if topic == self.topic.name:
                participants.append(id_)
        # 5.
        participants.sort()
        #print "participants: ", participants

        self.commit_offsets()

        # 6.
        i = participants.index(self.id)
        parts_per_consumer = len(self.topic.partitions) / len(participants)
        # TODO: deal with remainder
        #if i == len(participants) - 1:
        #    parts_per_consumer += len(self.topic.partitions) % len(participants)
        #print "ppc: ", parts_per_consumer

        # 7. assign partitions from i*N to (i+1)*N - 1 to consumer Ci
        new_partitions = itertools.islice(
            self.topic.partitions,
            i * parts_per_consumer,
            (i + 1) * parts_per_consumer
        )

        new_partitions = set(PartitionName.from_partition(p) for p in new_partitions)
        #print "new, ", new_partitions

        old_partitions = self.partition_owner_registry.get()
        #print "old: ", old_partitions

        # 8. remove current entries from the partition owner registry
        self.partition_owner_registry.remove(
            old_partitions - new_partitions
        )
        #print "to remove: ", old_partitions - new_partitions

        # 9. add newly assigned partitions to the partition owner registry
        for i in xrange(self.MAX_RETRIES):
            try:
                self.partition_owner_registry.add(
                    new_partitions - old_partitions
                )
                break
            except NodeExistsException:
                import time
                time.sleep(i ** 2)
                continue
        else:
            raise Exception("Couldn't acquire partitions.")

        self.partitions = self.partition_owner_registry.get()


    def __iter__(self):
        """
        Returns an iterator of messages.
        """

        partitions = itertools.ifilter(
            lambda p: PartitionName.from_partition(p) in self.partitions,
            self.topic.partitions
        )

        return itertools.chain.from_iterable(
            itertools.imap(
                lambda p: p.fetch(0, 1000),
                partitions
            )
        )

    def commit_offsets(self):
        """
        Commit the offsets of all messages consumed so far.
        """
        return
        raise NotImplementedError
