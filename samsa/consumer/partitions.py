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
import Queue

from samsa.config import ConsumerConfig
from samsa.exceptions import PartitionOwnedError
from samsa.partitions import Partition


logger = logging.getLogger(__name__)


class OwnedPartition(Partition):
    """Represents a partition as a consumer group sees it.

    Each partition owns a long-lived fetch thread that keeps
    and internal message queue full. It also has a simple
    monitor thread restart the fetch thread if it crashes

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

        # _current_offset is offset after the message most recently consumed
        try:
            offset, stat = self.cluster.zookeeper.get(self.path)
            self._current_offset = int(offset) if offset else 0
        except NoNodeException:
            self._current_offset = 0
            self.cluster.zookeeper.ensure_path(self.path)

        # the next offset to fetch --  initially the same as current
        self._next_offset = self._current_offset
        self._message_queue = Queue.Queue(self.config['queuedchunks_max'])

        # Fetch and monitor threads
        self._monitor_interval = 1 #: Time between monitoring checks
        self._monitor_thread = None #: Monitors the fetch thread
        self._fetch_thread = None #: Keep the partition queue full

        # Flag to stop returning messages or fetching more
        self._running = False


    @property
    def offset(self):
        return self._current_offset

    def _fetch(self):
        """Entry point for the fetch thread

        Runs as a long-lived thread that will keep the internal queue full.

        """
        while True:
            if not self._running:
                return

            # Fetch more messages
            messages = super(OwnedPartition, self).fetch(
                self._next_offset,
                self.config['fetch_size']
            )

            # Iterate over the list and try to put into _message_queue
            # but be sure to check _running even when blocking
            messages = deque(messages)
            while len(messages) > 0:
                message = messages.popleft() # deque adds items left-to-right
                if not self._running:
                    return # thread death
                try:
                    self._message_queue.put(
                        message,
                        block=True,
                        timeout=self._monitor_interval
                    )
                    self._next_offset = message.next_offset
                except Queue.Full:
                    # Put it back, check _running and then try again
                    messages.appendleft(message)

    def _monitor(self):
        """Makes sure that the fetch thread restarts on crash

        """
        while True:
            if not self._running:
                return
            if not self._fetch_thread or not self._fetch_thread.is_alive():
                self._fetch_thread = self.cluster.handler.spawn(target=self._fetch)
            time.sleep(self._monitor_interval)

    def empty(self):
        """True if there are no messages.

        """
        return self._message_queue.empty()

    def next_message(self, block=True, timeout=None):
        """Retrieve the next message for this partition.

        Returns None if the partition is stopped, or there are no messages
        within the timeout (or when not blocking)

        :param block: whether to block at all
        :param timeout: block for timeout if integer, or indefinitely if None.

        """
        if not self._running:
            return None

        try:
            message = self._message_queue.get(block=block, timeout=timeout)
            self._current_offset = message.next_offset
            return message.payload
        except Queue.Empty:
            return None

    def commit_offset(self):
        """Commit current offset to zookeeper.

        """
        self.cluster.zookeeper.set(self.path, str(self._current_offset))

    def start(self):
        """Start the fetch thread

        """
        self._running = True
        self._monitor_thread = self.cluster.handler.spawn(target=self._monitor)

    def stop(self):
        """Stop the fetch thread.

        """
        self._running = False

    def __del__(self):
        self.stop()


class PartitionOwnerRegistry(object):
    """Manages the Partition Owner Registry for a particular Consumer.

    This also helps manage access to the owned partitions. The Registry
    provides the ``get_ready_partition`` method that will give a consumer
    the next partition that has messages ready to be read. Using this,
    a consumer can iterate over active partitions without ever having to
    block while waiting on a less active one.

    Using the ready partition functionality, it's possible to multiplex
    between available partitions in a lock-free, highly available manner.
    To improve throughput, ideally a client would read several message from
    any given partition before moving on to the next.

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
        self.consumer = consumer
        self.cluster = cluster
        self.topic = topic

        self.Partition = partial(OwnedPartition, group=group)
        self.path = '/consumers/%s/owners/%s' % (group, topic.name)
        self.cluster.zookeeper.ensure_path(self.path)
        self._partitions = set([])
        self._ready_queue = Queue.Queue()
        self._watch_interval = 0.5

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
            p.stop() # TODO: Fix stop so it happens here instead of in consumer
            self.cluster.zookeeper.delete(self._path_from_partition(p))
            self._partitions.remove(p)

    def get_ready_partition(self, block=True, timeout=None):
        """Get the next partition with messages to read

        Returns None if none are available and nonblocking or timeout expires

        """
        # N.B.: Immediately setting the dequeued event means the watch
        # thread will unblock, see there are pending messages, and requeue
        # the partition. If a partition is exhausted during its read, then
        # it'll be empty the next time it's dequeued.
        #
        # This is by design. By not waiting to requeue the partition
        # there's no need to keep track of when it starts and stops usage.
        # That cuts down on monitoring overhead, reduces potential bugs due
        # to not letting go of the partition, and most importantly means
        # we can do the entire thing lock-free, using only Events.
        #
        # Also note the immediate-requeue only happens once. After that,
        # the watch thread will block until more messages are fetched.
        while True:
            try:
                partition, dequeued  = self._ready_queue.get(
                    block=block, timeout=timeout
                )
                dequeued.set()
            except Queue.Empty:
                return None

            # only return active partitions that still have messages
            if partition not in self._partitions or partition.empty():
                continue

            return partition

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
            self.cluster.handler.spawn(
                target=self._watch_partition, args=[partition]
            )

    def _path_from_partition(self, p):
        return "%s/%s-%s" % (self.path, p.broker.id, p.number)

    def _watch_partition(self, partition):
        """Watch the partition and put it in the ready_queue when it has data

        This watch thread (and ready queue) are how we can implement the
        partition multiplexing without lock-free and with very fast data
        availability

        """
        # N.B.: Everything in the main loop needs a timeout so it
        # can check for partition membership. Otherwise it never dies.
        import threading # TODO: Obv incompatible with gevent.
        dequeued = threading.Event()
        dequeued.set()
        while True:
            if partition not in self._partitions:
                return

            # Wait for it to be removed from the queue
            dequeued.wait(self._watch_interval)
            if not dequeued.is_set():
                continue

            # Wait for messages. Use of not_empty is from Queue.get source
            # TODO: Also very incompatible with gevent
            queue = partition._message_queue
            queue.not_empty.acquire()
            try:
                if queue._qsize() == 0:
                    queue.not_empty.wait(self._watch_interval)
                if queue._qsize() == 0:
                    continue # check partition membership
            finally:
                queue.not_empty.release()

            # ``.clear`` before ``.put`` avoids a race condition
            dequeued.clear()
            self._ready_queue.put((partition, dequeued))
