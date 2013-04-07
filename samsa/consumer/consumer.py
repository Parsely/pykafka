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
from samsa.exceptions import (SamsaException, NoAvailablePartitionsError,
                              PartitionOwnedError, ImproperlyConfiguredError)

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

        self._add_self()
        self._rebalance()

    def _add_self(self):
        """Add this consumer to the zookeeper participants.

        Ensures we don't add more participants than partitions
        """
        for i in xrange(self.config['consumer_retries_max'] or 1):
            time.sleep(i**2) # first run is 0, ensures we sleep before retry

            participants = self._get_others()
            if len(self.topic.partitions) > len(participants):
                break # some room to spare
            else:
                logger.debug("More consumers than partitions. "
                             "Waiting %is to retry" % (i+1) ** 2)
        else:
            raise NoAvailablePartitionsError("Couldn't acquire partition. "
                                             "More consumers than partitions.")

        path = '%s/%s' % (self.id_path, self.id)
        self.cluster.zookeeper.create(
            path, self.topic.name, ephemeral=True, makepath=True)

    def _get_others(self, watch=None):
        """Get a the other consumers of this topic"""
        zk = self.cluster.zookeeper
        try:
            consumer_ids = zk.get_children(self.id_path, watch=watch)
        except NoNodeException:
            logger.debug("Consumer group doesn't exist. No participants to find")
            return []

        participants = []
        for id_ in consumer_ids:
            try:
                topic, stat = zk.get("%s/%s" % (self.id_path, id_))
                if topic == self.topic.name:
                    participants.append(id_)
            except NoNodeException:
                pass # disappeared between ``get_children`` and ``get``
        participants.sort()
        return participants

    def _decide_partitions(self, participants):
        """Use consumers and partitions to determined owned partitions

        Give a set of subscribed consumers, every individual consumer should
        be able to figure out the same distribution of available partitions.

        It's very, very important this gives the same result on all machines,
        so things like participant and partition lists are always sorted.

        The algorithm is to distribute blocks of partitions based on
        how many participants there are. If there are partitions remaining,
        the last R participants get one extra, where R is the remainder.
        """
        # Freeze and sort partitions so we always have the same results
        p_to_str = lambda p: '-'.join(
            [p.topic.name, str(p.broker.id), str(p.number)]
        )
        all_partitions = list(self.topic.partitions)
        all_partitions.sort(key=p_to_str)

        # get start point, # of partitions, and remainder
        idx = participants.index(self.id)
        parts_per_consumer = len(all_partitions) / len(participants)
        remainder_ppc = len(all_partitions) % len(participants)

        start = parts_per_consumer * idx + min(idx, remainder_ppc)
        num_parts = parts_per_consumer + (0 if (idx + 1 > remainder_ppc) else 1)

        # assign partitions from i*N to (i+1)*N - 1 to consumer Ci
        new_partitions = itertools.islice(
            all_partitions,
            start,
            start + num_parts
        )
        new_partitions = set(new_partitions)
        logger.debug(
            'Rebalancing to %s based on %i participants %s and partitions %s',
            [p_to_str(p) for p in new_partitions],
            len(participants), str(participants),
            [p_to_str(p) for p in all_partitions]
        )
        return new_partitions



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

        # stop reading until post-rebalance
        self.stop_partitions()

        for i in xrange(self.config['rebalance_retries_max'] or 1):
            if i > 0:
                logger.debug("Retrying in %is" % ((i+1) ** 2))
                time.sleep(i ** 2)

            # Find owned partitions and remove old ones. Always
            # re-decide ownership after waiting since new consumers
            # may have joined while sleeping
            participants = self._get_others(watch=self._rebalance)
            new_partitions = self._decide_partitions(participants)
            self.partition_owner_registry.remove(
                self.partitions - new_partitions
            )

            try:
                self.partition_owner_registry.add(
                    new_partitions - self.partitions
                )
                break
            except PartitionOwnedError, e:
                logger.debug("Someone still owns partition %s.", e)
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
        partitions = list(self.partitions) # freeze in case of rebalance
        for partition in partitions:
            partition.commit_offset()

    def stop_partitions(self):
        """Stop partitions from fetching more threads.

        """
        self.commit_offsets()
        partitions = list(self.partitions) # freeze in case of rebalance
        for partition in partitions:
            partition.stop()

    def empty(self):
        return all([p.empty() for p in self.partitions])
