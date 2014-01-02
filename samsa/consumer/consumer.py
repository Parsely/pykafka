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
import itertools
import random
import socket
import time

from kazoo.recipe.watchers import ChildrenWatch
from kazoo.exceptions import NoNodeException
from uuid import uuid4

from samsa.consumer.partitions import PartitionOwnerRegistry
from samsa.exceptions import (SamsaException, NoAvailablePartitionsError,
                              PartitionOwnedError, ImproperlyConfiguredError)

logger = logging.getLogger(__name__)


class Consumer(object):
    """Primary API for consuming kazoo messages as a group.
    """

    def __init__(self,
                 cluster,
                 topic,
                 group,
                 backoff_increment=1,
                 connect_retries=4,
                 fetch_size=307200,
                 offset_reset='nearest',
                 rebalance_retries=4,
                 ):
        """
        For more info see: samsa.topics.Topic.subscribe

        :param cluster:
        :type cluster: :class:`samsa.cluster.Cluster`.
        :param topic: The topic to consume messages from.
        :type topic: :class:`samsa.topics.Topic`.
        :param group: The consumer group to join.
        :param backoff_increment: How fast to incrementally backoff when a
                                  partition has no messages to read.
        :param connect_retries: Retries before giving up on connecting
        :param fetch_size: Default fetch size (in bytes) to get from Kafka
        :param offset_reset: Where to reset when an OffsetOutOfRange happens
        :param rebalance_retries: Retries before giving up on rebalance
        :rtype: :class:`samsa.consumer.consumer.Consumer`
        """
        self.connect_retries = connect_retries
        self.rebalance_retries = rebalance_retries

        self.cluster = cluster
        self.topic = topic
        self.group = group
        self.id = "%s:%s" % (socket.gethostname(), uuid4())

        self.id_path = '/consumers/%s/ids' % self.group

        self.partition_owner_registry = PartitionOwnerRegistry(
            self, cluster, topic, group, backoff_increment=backoff_increment,
            fetch_size=fetch_size, offset_reset=offset_reset)
        self.partitions = self.partition_owner_registry.get()

        # Keep track of the partition being read and how much has been read
        self._current_partition = None
        self._current_read_ct = 0

        # Watches
        # TODO: This is a *ton* of watches, some of which are duplicated
        #       elsewhere. This should be cleaned up and all watches put
        #       in a single zookeeper connector, like in the Scala driver.
        self._broker_watcher = None
        self._consumer_watcher = None
        self._topic_watcher = None
        self._topics_watcher = None
        self._rebalancing = True # To stop rebalance while setting watches

        self._add_self()


    def _add_self(self):
        """Add this consumer to the zookeeper participants.

        Ensures we don't add more participants than partitions
        """
        for i in xrange(self.connect_retries):
            time.sleep(i**2) # first run is 0, ensures we sleep before retry

            participants = self._get_participants()
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

        # Set all our watches and then rebalance
        self._rebalancing = False
        broker_path = '/brokers/ids'
        try:
            self._broker_watcher = ChildrenWatch(
                self.cluster.zookeeper, broker_path,
                self._brokers_changed
            )
        except NoNodeException:
            raise ImproperlyConfiguredError(
                'The broker_path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?'
                % broker_path)

        topics_path = '/brokers/topics'
        self._topics_watcher = ChildrenWatch(
            self.cluster.zookeeper,
            '/brokers/topics',
            self._topics_changed
        )
        self._rebalancing = True

        # Final watch will trigger rebalance
        self._consumer_watcher = ChildrenWatch(
            self.cluster.zookeeper, self.id_path,
            self._consumers_changed
        )


    def _brokers_changed(self, brokers):
        """Watcher for consumer group changes"""
        if not self._rebalancing:
            return
        logger.info("Rebalance triggered by /brokers/ids change")
        self._rebalance(self.cluster.zookeeper.get_children(self.id_path))


    def _consumers_changed(self, consumer_ids):
        """Watcher for consumer group changes

        """
        if not self._rebalancing:
            return
        logger.info("Rebalance triggered by %s change" % self.id_path)
        self._rebalance(consumer_ids)


    def _topic_changed(self, broker_ids):
        """Watcher for brokers/partition count for a topic

        """
        if not self._rebalancing:
            return
        topic_path = '/brokers/topics/%s' % self.topic.name
        logger.info("Rebalance triggered by %s change" % topic_path)
        self._rebalance(self.cluster.zookeeper.get_children(self.id_path))

    def _topics_changed(self, topics):
        """Watch for the topic we want to show up, then stop watch

        """
        if self.topic.name in topics:
            self._topic_watcher = ChildrenWatch(
                self.cluster.zookeeper,
                '/brokers/topics/%s' % self.topic.name,
                self._topic_changed
            )
            return False # stop watch

    def _get_participants(self, consumer_ids=None):
        """Get a the other consumers of this topic

        :param consumer_ids: List of consumer_ids (from ChildrenWatch)
        """
        zk = self.cluster.zookeeper
        if not consumer_ids:
            try:
                consumer_ids = zk.get_children(self.id_path)
            except NoNodeException:
                logger.debug("Consumer group doesn't exist. "
                             "No participants to find")
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
        logger.info(
            'Rebalancing %i participants for %i partitions. '
            'My Partitions: %s -- Consumers: %s --- All Partitions: %s',
            len(participants), len(all_partitions),
            [p_to_str(p) for p in new_partitions],
            str(participants),
            [p_to_str(p) for p in all_partitions]
        )
        return new_partitions


    def _rebalance(self, consumer_ids):
        """Joins a consumer group and claims partitions.

        """
        logger.info('Rebalancing consumer %s for topic %s.' % (
            self.id, self.topic.name)
        )

        participants = self._get_participants(consumer_ids=consumer_ids)
        new_partitions = self._decide_partitions(participants)

        for i in xrange(self.rebalance_retries):
            if i > 0:
                logger.debug("Retrying in %is" % ((i+1) ** 2))
                time.sleep(i ** 2)
                # Make sure nothing's changed while we waited
                participants = self._get_participants()
                new_partitions = self._decide_partitions(participants)

            # Remove old partitions and acquire new ones.
            old_partitions = self.partitions - new_partitions
            self.stop_partitions(partitions=old_partitions)
            self.partition_owner_registry.remove(old_partitions)

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
        """Iterator for next_message. Blocks until a message arrives

        """
        while True:
            yield self.next_message()


    def next_message(self, block=True, timeout=None):
        """Get the next message from one of the partitions.

        This works by picking a partition, reading a bunch of messages
        from it, and then switching to the next partition with messages
        ready. To configure how many are read at a time, use the
        setting reads_per_partition.

        Returns None if no messages are ready and timeout expires

        :param block: Whether to block while waiting for a message
        :param timeout: How long to wait, in seconds, if blocking

        """
        return self.partition_owner_registry.next_message(block=block, timeout=timeout)


    def commit_offsets(self):
        """Commit the offsets of all messages consumed so far.

        """
        partitions = list(self.partitions) # freeze in case of rebalance
        for partition in partitions:
            partition.commit_offset()


    def stop_partitions(self, partitions=None):
        """Stop partitions from fetching more threads.

        :param partitions: Partitions to remove. (default: self.partitions)

        """
        if partitions is None:
            partitions = list(self.partitions) # freeze in case of rebalance
        for partition in partitions:
            partition.stop()
            partition.commit_offset()


    def empty(self):
        return all([p.empty() for p in self.partitions])
