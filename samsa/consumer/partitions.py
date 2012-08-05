__license__ = """
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

import threading

from kazoo.exceptions import NodeExistsException, NoNodeException
from functools import partial
from Queue import Queue

from samsa.config import ConsumerConfig
from samsa.exceptions import PartitionOwnedException
from samsa.partitions import Partition


class OwnedPartition(Partition):

    """Represents a partition as a consumer group sees it.

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

        self.config = ConsumerConfig().build()

        self.group = group
        self.path = "/consumers/%s/offsets/%s/%s-%s" % (
            self.group, self.topic.name,
            self.broker.id, self.number
        )

        try:
            offset, stat = self.cluster.zookeeper.get(self.path)
            self._offset = int(offset)
        except NoNodeException:
            self.cluster.zookeeper.create(self.path, str(0), makepath=True)
            self._offset = 0

        self.queue = Queue(self.config['queuedchunks_max'])
        self.fetch_thread = self._create_thread()

    def _create_thread(self):
        fetch_thread = threading.Thread(
            target=self._fetch,
            args=(self.config['fetch_size'],)
        )
        fetch_thread.daemon = True
        fetch_thread.start()
        return fetch_thread

    def _fetch(self, size):
        """Fetch up to `size` bytes of new messages and add to queue.

        :param size: size in bytes of new messages to fetch.
        :type size: int

        """
        messages = super(OwnedPartition, self).fetch(self._offset, size)
        for message in messages:
            self.queue.put(
                message, True,
                self.config['consumer_timeout']
            )

    def next_message(self, timeout=None):
        """Retrieve the next message for this partition.

        :param timeout: blog for timeout if integer, or indefinitely if None.

        """

        if not self.fetch_thread.is_alive():
            self.fetch_thread = self._create_thread()
        if not timeout:
            timeout = self.config['consumer_timeout']

        msg = self.queue.get(True, timeout)
        self._offset = msg.next_offset
        return msg.payload

    def commit_offset(self):
        """Commit current offset to zookeeper.
        """
        self.cluster.zookeeper.set(self.path, str(self._offset))

    def stop(self):
        self.fetch_thread.join()

    def __del__(self):
        self.stop()


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
        :type partitions: iterable of :class:`samsa.partitions.Partition`.

        """
        for p in partitions:
            assert p in self._partitions
            self.cluster.zookeeper.delete(self._path_from_partition(p))
            self._partitions.remove(p)

    def add(self, partitions):
        """Add `partitions` to the registry.

        :param partitions: partitions to add.
        :type partitions: iterable of :class:`samsa.partitions.Partition`.

        """
        for p in partitions:
            try:
                self.cluster.zookeeper.create(
                    self._path_from_partition(p), self.consumer_id,
                    ephemeral=True
                )
            except NodeExistsException:
                raise PartitionOwnedException(p)
            self._partitions.add(self.Partition(p))

    def _path_from_partition(self, p):
        return "%s/%s-%s" % (self.path, p.broker.id, p.number)
