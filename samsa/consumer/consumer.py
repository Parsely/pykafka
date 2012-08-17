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

import logging
import itertools
import random
import socket
import time

from kazoo.exceptions import NoNodeException
from uuid import uuid4

from samsa.config import ConsumerConfig
from samsa.consumer.partitions import PartitionOwnerRegistry
from samsa.exceptions import (SamsaException, PartitionOwnedError,
                              ImproperlyConfiguredError)

logger = logging.getLogger(__name__)


class Consumer(object):
    """Primary API for consuming kazoo messages as a group.
    """

    def __init__(self, cluster, topic, group):
        """
        :param cluster:
        :type cluster: :class:`samsa.cluster.Cluster`.
        :param topic: The topic to consume messages from.
        :type topic: :class:`samsa.topics.Topic`.
        :param group: The consumer group to join.
        :type group: str.

        """
        self.config = ConsumerConfig().build()
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
            raise ImproperlyConfiguredError(
                'The broker_path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?'
                % broker_path)

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

        self.stop_partitions()

        # 8. remove current entries from the partition owner registry
        self.partition_owner_registry.remove(
            self.partitions - new_partitions
        )

        # 9. add newly assigned partitions to the partition owner registry
        for i in xrange(self.config['rebalance_retries_max']):
            try:
                # N.B. self.partitions will always reflect the most current
                # view of owned partitions. Therefor retrying this method
                # will progress.
                self.partition_owner_registry.add(
                    new_partitions - self.partitions
                )
                break
            except PartitionOwnedError, e:
                logger.debug("Someone still owns partition %s. Retrying"
                             % e)
                time.sleep(i ** 2)
                continue
        else:
            raise SamsaException("Couldn't acquire partitions.")

    def __iter__(self):
        """Iterate over available messages. Does not return.

        """
        while True:
            msg = self.next_message(self.config['consumer_timeout'])
            if not msg:
                time.sleep(1)
            else:
                yield msg

    def next_message(self, timeout=None):
        """Get the next message from one of the partitions.

        """
        return random.sample(self.partitions, 1)[0].next_message(timeout)

    def commit_offsets(self):
        """Commit the offsets of all messages consumed so far.

        """
        for partition in self.partitions:
            partition.commit_offset()

    def stop_partitions(self):
        """Stop partitions from fetching more threads.

        """
        self.commit_offsets()
        for partition in self.partitions:
            partition.stop()

    def empty(self):
        return all([p.empty() for p in self.partitions])
