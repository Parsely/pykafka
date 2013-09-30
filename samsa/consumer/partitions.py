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
import time

from collections import deque
from kazoo.exceptions import NodeExistsException, NoNodeException
from functools import partial

from samsa.client import OFFSET_EARLIEST, OFFSET_LATEST
from samsa.exceptions import OffsetOutOfRangeError, PartitionOwnedError
from samsa.partitions import Partition


logger = logging.getLogger(__name__)


class OwnedPartition(Partition):
    """Represents a partition as a consumer group sees it.

    Runs a long-lived fetcher thread that will fill a message queue
    with sets of messages from the Kafka server.

    Iterables of messages are used instead of individual messages to reduce
    thread contention for the central Queue.
    """

    def __init__(self,
                 partition,
                 group,
                 message_set_queue,
                 backoff_increment=1,
                 fetch_size=307200,
                 offset_reset='nearest'):
        """Initialize with a partition and consumer group.

        :param partition: Partition to initialize with.
        :type partition: :class:`samsa.partitions.Partition`.
        :param group: Group that owns this partition.
        :type group: str.
        :param backoff_increment: How fast to incrementally backoff when a
                                  partition has no messages to read.
        :param fetch_size: Default fetch size (in bytes) to get from Kafka
        :param offset_reset: Where to reset when an OffsetOutOfRange happens
        :param message_set_queue: The Queue to fill with message sets.
        """
        super(OwnedPartition, self).__init__(
            partition.cluster, partition.topic,
            partition.broker, partition.number
        )

        self.backoff_increment = backoff_increment
        self.fetch_size = fetch_size
        self.offset_reset = offset_reset

        self.group = group
        self.message_set_queue = message_set_queue
        self.path = "/consumers/%s/offsets/%s/%s-%s" % (
            self.group, self.topic.name,
            self.broker.id, self.number
        )

        # _current_offset is offset after the message most recently consumed
        try:
            offset, stat = self.cluster.zookeeper.get(self.path)
            self._current_offset = int(offset) if offset else 0
        except NoNodeException:
            self._current_offset = 0
            self.cluster.zookeeper.ensure_path(self.path)

        # the next offset to fetch --  initially the same as current
        self._next_offset = self._current_offset

        # Limit how many message sets we'll queue up
        self.queued_message_sets = self.cluster.handler.get_semaphore(10)
        # Fetch and monitor threads
        self._monitor_thread = None #: Monitors the fetch thread
        self._fetch_thread = None #: Keep the partition queue full
        # Flag to stop returning messages or fetching more
        self._running = False

    @property
    def offset(self):
        return self._current_offset

    def _start_fetch(self):
        """Entry point for the fetch thread

        Runs as a long-lived thread that will keep the internal queue full.
        """
        backoff = 0
        while True:
            if not self._running:
                return

            # Fetch more messages
            try:
                messages = super(OwnedPartition, self).fetch(
                    self._next_offset,
                    self.fetch_size
                )
            except OffsetOutOfRangeError, ex:
                msg = 'Offset %i is out of range. Resetting to %%s (%%d)' % self._next_offset
                reset = self.offset_reset
                if reset == 'nearest': # resolve which way this is going
                    if self._next_offset < self.earliest_offset():
                        reset = 'earliest'
                    else:
                        reset = 'latest'
                if reset == 'earliest':
                    self._next_offset = self.earliest_offset()
                    logger.warning(msg, 'OFFSET_EARLIEST', self._next_offset)
                    continue
                elif reset == 'latest':
                    self._next_offset = self.latest_offset()
                    logger.warning(msg, 'OFFSET_LATEST', self._next_offset)
                    continue
                else:
                    raise # no reset match? send it up to the consumer

            # If there are no messages read, back off a bit
            if len(messages) == 0:
                backoff += self.backoff_increment
                logger.debug('No messages ready. Sleeping for %ds', backoff)
                time.sleep(backoff)
                continue
            else:
                backoff = 0 # reset

            # Is there room in the waiting queue?
            self.queued_message_sets.acquire()
            self.message_set_queue.put(self._message_set_iter(messages))

            # Next place to fetch from
            self._next_offset = messages[-1].next_offset

    def _message_set_iter(self, messages):
        # time to get more messages
        self.queued_message_sets.release()
        for message in messages:
            self._current_offset = message.next_offset
            yield self, message

    def _monitor(self):
        """Makes sure that the fetch thread restarts on crash"""
        while True:
            if not self._running:
                return
            if not self._fetch_thread or not self._fetch_thread.is_alive():
                self._fetch_thread = self.cluster.handler.spawn(target=self._start_fetch)
            time.sleep(1)

    def commit_offset(self):
        """Commit an offset for the partition to zookeeper"""
        self.cluster.zookeeper.set(self.path, str(self._current_offset))

    def start(self):
        """Start fetching and filling the provided queue"""
        self._running = True
        self._monitor_thread = self.cluster.handler.spawn(target=self._monitor)

    def stop(self):
        """Stop the fetch thread"""
        self._running = False

    def __del__(self):
        self.stop()


class PartitionOwnerRegistry(object):
    """Manages the set of partitions a consumer reads from.

    This also handles multiplexing between the individual partitions
    that are reading from Kafka. It does this using an internal message
    set Queue that is filled by the OwnedPartitions.
    """

    def __init__(self,
                 consumer,
                 cluster,
                 topic,
                 group,
                 backoff_increment=1,
                 fetch_size=307200,
                 offset_reset='nearest',
                 ):
        """
        For more info see: samsa.topics.Topic.subscribe

        :param consumer: consumer which owns these partitions.
        :type consumer: :class:`samsa.consumer.Consumer`.
        :param cluster:
        :type cluster: :class:`samsa.cluster.Cluster`.
        :param topic: The topic the partitions belong to.
        :type topic: :class:`samsa.topics.Topic`.
        :param group: The group the partitions belong to.
        :type group: str.
        :param backoff_increment: How fast to incrementally backoff when a
                                  partition has no messages to read.
        :param fetch_size: Default fetch size (in bytes) to get from Kafka
        :param offset_reset: Where to reset when an OffsetOutOfRange happens
        """
        self.consumer = consumer
        self.cluster = cluster
        self.topic = topic

        self._message_queue = self.cluster.handler.Queue()
        self.Partition = partial(OwnedPartition,
                                 group=group,
                                 backoff_increment=backoff_increment,
                                 fetch_size=fetch_size,
                                 message_set_queue=self._message_queue,
                                 offset_reset=offset_reset)
        self.path = '/consumers/%s/owners/%s' % (group, topic.name)
        self.cluster.zookeeper.ensure_path(self.path)
        self._partitions = set([])
        self._current_message_set = None
        self._watch_interval = 0.5

    def _path_from_partition(self, p):
        return "%s/%s-%s" % (self.path, p.broker.id, p.number)

    def get(self):
        """Get all owned partitions"""
        return self._partitions

    def remove(self, partitions):
        """Remove `partitions` from the registry.

        :param partitions: partitions to remove.
        :type partitions: iterable of :class:`samsa.partitions.Partition`.
        """
        for p in partitions:
            assert p in self._partitions
            p.stop() # TODO: Fix stop so it happens here instead of in consumer
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
                    self._path_from_partition(p), self.consumer.id,
                    ephemeral=True
                )
            except NodeExistsException:
                raise PartitionOwnedError(p)

            # Add to self.partition and start the monitor
            partition = self.Partition(p)
            partition.start()
            self._partitions.add(partition)

    def next_message(self, block=True, timeout=None):
        # Don't exit until we have something
        while True:
            if not self._current_message_set:
                try:
                    self._current_message_set = self._message_queue.get(
                        block=block, timeout=timeout
                    )
                except self.cluster.handler.QueueEmptyError:
                    return None

            try:
                partition, message = self._current_message_set.next()
                if partition in self._partitions:
                    return message.payload
            except StopIteration:
                self._current_message_set = None
