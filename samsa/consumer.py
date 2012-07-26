import logging
import itertools
import socket
import time

from kazoo.exceptions import NodeExistsException, NoNodeException
from uuid import uuid4

from samsa.partitions import Partition
from samsa.exceptions import ImproperlyConfigured
from samsa.utils.delayedconfig import DelayedConfiguration, requires_configuration

logger = logging.getLogger(__name__)


class PartitionOwnedException(Exception): pass


class PartitionOwnerRegistry(DelayedConfiguration):
    """
    Manages the Partition Owner Registry for a particular Consumer.
    """

    def __init__(self, consumer, cluster, topic, group):
        self.consumer_id = str(consumer.id)
        self.cluster = cluster
        self.topic = topic
        self.path = '/consumers/%s/owners/%s' % (group, topic.name)
        self.cluster.zookeeper.ensure_path(self.path)
        self._partitions = set([])

    def _configure(self, event=None):
        zk = self.cluster.zookeeper
        partitions = zk.get_children(self.path, watch=self._configure)
        new_partitions = set([])

        for name in partitions:
            p = self._partition_from_name(name)

            try:
                value, _ = zk.get(self._path_from_partition(p))
            except NoNodeException:
                # some other consumer has removed this node. it's not ours.
                continue
            if value == self.consumer_id:
                new_partitions.add(p)

        # we want references to self._partitions to not change.
        self._partitions.difference_update(self._partitions - new_partitions)
        self._partitions.update(new_partitions)

    @requires_configuration
    def get(self):
        return self._partitions

    @requires_configuration
    def remove(self, partitions):
        for p in partitions:
            assert p in self._partitions
            self.cluster.zookeeper.delete(self._path_from_partition(p))
            self._partitions.remove(p)

    @requires_configuration
    def add(self, partitions):
        for p in partitions:
            try:
                self.cluster.zookeeper.create(
                    self._path_from_partition(p), self.consumer_id, ephemeral=True
                )
            except NodeExistsException:
                raise PartitionOwnedException(p)
            self._partitions.add(p)

    def _path_from_partition(self, p):
        return "%s/%s-%s" % (self.path, p.broker.id, p.number)

    def _partition_from_name(self, name):
        """name as it appears as a znode. <broker id>-<parititon number>."""
        broker_id, partition_id = name.split('-')
        broker = self.cluster.brokers[int(broker_id)]
        return Partition(self.cluster, self.topic, broker, partition_id)


class Consumer(object):

    MAX_RETRIES = 5

    def __init__(self, cluster, topic, group):
        self.cluster = cluster
        self.topic = topic
        self.group = group
        self.id = "%s:%s" % (socket.gethostname(), uuid4())

        self.id_path = '/consumers/%s/ids' % self.group

        self.partition_owner_registry = PartitionOwnerRegistry(
            self, cluster, topic, group)
        self.partitions = self.partition_owner_registry.get()

        path = '%s/%s' % (self.id_path, self.id)
        self.cluster.zookeeper.ensure_path(self.id_path)
        self.cluster.zookeeper.create(path, self.topic.name, ephemeral=True)

        self._rebalance()

    def _rebalance(self, event=None):
        """
        Joins a consumer group and claims partitions.
        """

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

        # 6.
        i = participants.index(self.id)
        parts_per_consumer = len(self.topic.partitions) / len(participants)
        remainder_ppc = len(self.topic.partitions) % len(participants)

        start = parts_per_consumer * i + min(i, remainder_ppc)
        num_parts = parts_per_consumer + (0 if (i + 1 > remainder_ppc) else 1)

        # 7. assign partitions from i*N to (i+1)*N - 1 to consumer Ci
        new_partitions = itertools.islice(
            self.topic.partitions,
            start,
            start + num_parts
        )

        new_partitions = set(new_partitions)

        self.commit_offsets()

        # 8. remove current entries from the partition owner registry
        self.partition_owner_registry.remove(
            self.partitions - new_partitions
        )

        # 9. add newly assigned partitions to the partition owner registry
        for i in xrange(self.MAX_RETRIES):
            try:
                # self.partitions will always reflect the most current view of
                # owned partitions. Therefor retrying this method will progress.
                self.partition_owner_registry.add(
                    new_partitions - self.partitions
                )
                break
            except PartitionOwnedException, e:
                # print ("Someone still owns partition %s. Retrying" %
                #        str(e.message))
                time.sleep(i ** 2)
                continue
        else:
            raise Exception("Couldn't acquire partitions.")


    def __iter__(self):
        """
        Returns an iterator of messages.
        """

        # fetch size is the kafka default.
        return itertools.chain.from_iterable(
            itertools.imap(
                lambda p: p.fetch(0, 300 * 1024),
                self.topic.partitions
            )
        )

    def commit_offsets(self):
        """
        Commit the offsets of all messages consumed so far.
        """
        return
