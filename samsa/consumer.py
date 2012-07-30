"""
Copyright 2012 DISQUS

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
import itertools
import socket
import time

from kazoo.exceptions import NodeExistsException, NoNodeException
from functools import partial
from uuid import uuid4

from samsa.partitions import Partition
from samsa.exceptions import ImproperlyConfigured

logger = logging.getLogger(__name__)


class PartitionOwnedException(Exception): pass

class OwnedPartition(Partition):
    """Represents a consumer group partition.

    Manages offset tracking and message fetching.
    """

    def __init__(self, partition, group):
        """Initialize with a partition and consumer group.

        :param partition: Partition to initialize with.
        :type partition: :class:`samsa.partitions.Partition`.
        :param group: Group that owns this partition.
        :type group: str.

        """

        super(OwnedPartition, self).__init__(
            partition.cluster, partition.topic,
            partition.broker, partition.number
        )
        self.group = group
        self.path = "/consumers/%s/offsets/%s/%s-%s" % (
            self.group, self.topic.name,
            self.broker.id, self.number
        )

        try:
            offset, stat = self.cluster.zookeeper.get(self.path)
            self.offset = int(offset)
        except NoNodeException:
            self.cluster.zookeeper.create(self.path, str(0), makepath=True)
            self.offset = 0

    def fetch(self, size):
        """Fetch up to `size` bytes of new messages.

        :param size: size in bytes of new messages to fetch.
        :type size: int
        :returns: generator -- message iterator.

        """

        messages = super(OwnedPartition, self).fetch(self.offset, size)
        last_offset = 0
        for offset, msg in messages:
            # offset is relative to this response.
            self.offset += offset - last_offset
            last_offset = offset
            yield msg

    def commit_offset(self):
        """Commit current offset to zookeeper.
        """

        self.cluster.zookeeper.set(self.path, str(self.offset))


class PartitionOwnerRegistry(object):
    """Manages the Partition Owner Registry for a particular Consumer.
    """

    def __init__(self, consumer, cluster, topic, group):
        """
        :param consumer: consumer which owns these partitions.
        :type consumer: :class:`samsa.consumer.Consumer`.
        :param cluster:
        :type cluster: :class:`samsa.cluster.Cluster`.
        :param topic: The topic the partitions belong to.
        :type topic: :class:`samsa.topics.Topic`.
        :param group: The group the partitions belong to.
        :type group: str.

        """

        self.consumer_id = str(consumer.id)
        self.cluster = cluster
        self.topic = topic

        self.Partition = partial(OwnedPartition, group=group)
        self.path = '/consumers/%s/owners/%s' % (group, topic.name)
        self.cluster.zookeeper.ensure_path(self.path)
        self._partitions = set([])

    def get(self):
        """Get all owned partitions.
        """

        return self._partitions

    def remove(self, partitions):
        """Remove `partitions` from the registry.

        :param partitions: partitions to remove.
        :type partitions: iterable.

        """

        for p in partitions:
            assert p in self._partitions
            self.cluster.zookeeper.delete(self._path_from_partition(p))
            self._partitions.remove(p)

    def add(self, partitions):
        """Add `partitions` to the registry.

        :param partitions: partitions to add.
        :type partitions: iterable.

        """

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
        """name as it appears as a znode. <broker id>-<partition number>."""
        broker_id, partition_id = name.split('-')
        broker = self.cluster.brokers[int(broker_id)]
        p = Partition(self.cluster, self.topic, broker, partition_id)
        return self.Partition(p)


class Consumer(object):
    """Primary API for consuming kazoo messages as a group.
    """

    MAX_RETRIES = 5

    def __init__(self, cluster, topic, group):
        """
        :param cluster:
        :type cluster: :class:`samsa.cluster.Cluster`.
        :param topic: The topic to consume messages from.
        :type topic: :class:`samsa.topics.Topic`.
        :param group: The consumer group to join.
        :type group: str.

        """

        self.cluster = cluster
        self.topic = topic
        self.group = group
        self.id = "%s:%s" % (socket.gethostname(), uuid4())

        self.id_path = '/consumers/%s/ids' % self.group

        self.partition_owner_registry = PartitionOwnerRegistry(
            self, cluster, topic, group)
        self.partitions = self.partition_owner_registry.get()

        path = '%s/%s' % (self.id_path, self.id)
        self.cluster.zookeeper.create(
            path, self.topic.name, ephemeral=True, makepath=True
        )

        self._rebalance()

    def _rebalance(self, event=None):
        """Joins a consumer group and claims partitions.
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

        # cast partitions to owned partitions.
        new_partitions = itertools.imap(
            self.partition_owner_registry.Partition,
            new_partitions
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
                # N.B. self.partitions will always reflect the most current view of
                # owned partitions. Therefor retrying this method will progress.
                self.partition_owner_registry.add(
                    new_partitions - self.partitions
                )
                break
            except PartitionOwnedException, e:
                logger.debug("Someone still owns partition %s. Retrying" %
                        str(e.message))
                time.sleep(i ** 2)
                continue
        else:
            raise Exception("Couldn't acquire partitions.")


    def __iter__(self):
        """Returns an iterator of messages.
        """

        # fetch size is the kafka default.
        return itertools.chain.from_iterable(
                itertools.imap(
                lambda p: p.fetch(300 * 1024),
                self.partitions
            )
        )

    def commit_offsets(self):
        """Commit the offsets of all messages consumed so far.
        """

        for partition in self.partitions:
            partition.commit_offset()
