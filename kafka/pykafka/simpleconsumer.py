import functools
import itertools
from collections import defaultdict
import time
import logging as log
from Queue import Queue, Empty
import weakref
import threading

from kafka import base
from kafka.common import OffsetType

from .protocol import (PartitionFetchRequest, PartitionOffsetCommitRequest,
                       PartitionOffsetFetchRequest)


class SimpleConsumer(base.BaseSimpleConsumer):
    def __init__(self,
                 topic,
                 cluster,
                 consumer_group=None,
                 partitions=None,
                 fetch_message_max_bytes=1024 * 1024,
                 num_consumer_fetchers=1,
                 auto_commit_enable=False,
                 auto_commit_interval_ms=60 * 1000,
                 queued_max_messages=2000,
                 fetch_min_bytes=1,
                 fetch_wait_max_ms=100,
                 refresh_leader_backoff_ms=200,
                 offsets_channel_backoff_ms=1000,
                 offsets_commit_max_retries=5,
                 auto_offset_reset=OffsetType.LATEST,
                 consumer_timeout_ms=-1):
        """Create a SimpleConsumer.

        Settings and default values are taken from the Scala
        consumer implementation.  Consumer group is included
        because it's necessary for offset management, but doesn't imply
        that this is a balancing consumer. Use a BalancedConsumer for
        that.

        :param topic: the topic this consumer should consume
        :type topic: pykafka.topic.Topic
        :param cluster: the cluster this consumer should connect to
        :type cluster: pykafka.cluster.Cluster
        :param consumer_group: the name of the consumer group to join
        :type consumer_group: str
        :param partitions: existing partitions to which to connect
        :type partitions: list of pykafka.partition.Partition
        :param fetch_message_max_bytes: the number of bytes of messages to
            attempt to fetch
        :type fetch_message_max_bytes: int
        :param num_consumer_fetchers: the number of threads used to fetch data
        :type num_consumer_fetchers: int
        :param auto_commit_enable: if true, periodically commit to kafka the
            offset of messages already fetched by this consumer
        :type auto_commit_enable: bool
        :param auto_commit_interval_ms: the frequency in ms that the consumer
            offsets are committed to kafka
        :type auto_commit_interval_ms: int
        :param queued_max_messages: max number of messages buffered for
            consumption
        :type queued_max_messages: int
        :param fetch_min_bytes: the minimum amount of data the server should
            return for a fetch request. If insufficient data is available the
            request will block
        :type fetch_min_bytes: int
        :param fetch_wait_max_ms: the maximum amount of time the server will
            block before answering the fetch request if there isn't sufficient
            data to immediately satisfy fetch_min_bytes
        :type fetch_wait_max_ms: int
        :param refresh_leader_backoff_ms: backoff time to refresh the leader of
            a partition after it loses the current leader
        :type refresh_leader_backoff_ms: int
        :param offsets_channel_backoff_ms: backoff time to retry offset
            commits/fetches
        :type offsets_channel_backoff_ms: int
        :param offsets_commit_max_retries: Retry the offset commit up to this
            many times on failure.
        :type offsets_commit_max_retries: int
        :param auto_offset_reset: what to do if an offset is out of range
        :type auto_offset_reset: int
        :param consumer_timeout_ms: throw a timeout exception to the consumer
            if no message is available for consumption after the specified interval
        :type consumer_timeout_ms: int
        """
        if not isinstance(cluster, weakref.ProxyType):
            self._cluster = weakref.proxy(cluster)
        else:
            self._cluster = cluster
        self._consumer_group = consumer_group
        self._topic = topic
        self._fetch_message_max_bytes = fetch_message_max_bytes
        self._fetch_min_bytes = fetch_min_bytes
        self._queued_max_messages = queued_max_messages
        self._num_consumer_fetchers = num_consumer_fetchers
        self._fetch_wait_max_ms = fetch_wait_max_ms
        self._consumer_timeout_ms = consumer_timeout_ms

        self._last_message_time = time.time()

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._last_auto_commit = time.time()

        self._running = True

        self._offset_manager = self._cluster.get_offset_manager(self._consumer_group)

        owned_partition_partial = functools.partial(
            OwnedPartition, consumer_group=self._consumer_group)
        if partitions:
            self._partitions = {owned_partition_partial(p): p
                                for p in partitions}
        else:
            self._partitions = {owned_partition_partial(p): topic.partitions[k]
                                for k, p in topic.partitions.iteritems()}
        self._partitions_by_id = {p.partition.id: p
                                  for p in self._partitions.iterkeys()}
        # Organize partitions by leader for efficient queries
        self._partitions_by_leader = defaultdict(list)
        for p in self._partitions.iterkeys():
            self._partitions_by_leader[p.partition.leader].append(p)
        self.partition_cycle = itertools.cycle(self._partitions.keys())

        if self._auto_commit_enable:
            self._autocommit_worker_thread = self._setup_autocommit_worker()
        # we need to get the most up-to-date offsets before starting consumption
        self.fetch_offsets()
        self._fetch_workers = self._setup_fetch_workers()

    @property
    def topic(self):
        return self._topic

    @property
    def partitions(self):
        return self._partitions

    def __del__(self):
        self.stop()

    def stop(self):
        self._running = False

    def _setup_autocommit_worker(self):
        def autocommitter():
            while True:
                if not self._running:
                    break
                if self._auto_commit_enable:
                    self._auto_commit()
        log.debug("Starting autocommitter thread")
        return self._cluster.handler.spawn(autocommitter)

    def _setup_fetch_workers(self):
        def fetcher():
            while True:
                if not self._running:
                    break
                self.fetch()
        return [self._cluster.handler.spawn(fetcher)
                for i in xrange(self._num_consumer_fetchers)]

    def __iter__(self):
        while True:
            message = self.consume()
            if not message and self._consumer_timed_out():
                raise StopIteration
            yield message

    def _consumer_timed_out(self):
        if self._consumer_timeout_ms == -1:
            return False
        disp = (time.time() - self._last_message_time) * 1000.0
        return disp > self._consumer_timeout_ms

    def consume(self):
        """Get one message from the consumer.

        :param timeout: Seconds to wait before returning None
        """
        owned_partition = self.partition_cycle.next()
        message = owned_partition.consume()

        if message:
            self._last_message_time = time.time()
            return message

    def _auto_commit(self):
        if not self._auto_commit_enable or self._auto_commit_interval_ms == 0:
            return

        if (time.time() - self._last_auto_commit) * 1000.0 >= self._auto_commit_interval_ms:
            log.info("Autocommitting consumer offset for consumer group %s and topic %s",
                     self._consumer_group, self._topic.name)
            self.commit_offsets()
            self._last_auto_commit = time.time()

    def commit_offsets(self):
        """Commit offsets for this consumer's topic

        Uses the offset commit/fetch API
        """
        if not self._consumer_group:
            raise Exception("consumer group must be specified to commit offsets")

        self._offset_manager = self._cluster.get_offset_manager(self._consumer_group)

        reqs = [p.build_offset_commit_request() for p in self._partitions.keys()]
        log.info("Committing offsets for %d partitions", len(reqs))
        self._offset_manager.commit_consumer_group_offsets(
            self._consumer_group, 1, 'pykafka', reqs)

    def fetch_offsets(self):
        """Fetch offsets for this consumer's topic

        Uses the offset commit/fetch API
        Should be called when consumer starts and after any errors
        """
        if not self._consumer_group:
            raise Exception("consumer group must be specified to fetch offsets")

        self._offset_manager = self._cluster.get_offset_manager(self._consumer_group)

        log.info("Fetching offsets")

        reqs = [p.build_offset_fetch_request() for p in self._partitions.keys()]
        res = self._offset_manager.fetch_consumer_group_offsets(self._consumer_group, reqs)
        for partition_id, pres in res.topics[self._topic.name].iteritems():
            partition = self._partitions_by_id[partition_id]
            partition.set_offset_counters(pres)

    def fetch(self):
        """Fetch new messages for all partitions

        Create a FetchRequest for each broker and send it. Enqueue each of the
        returned messages in the approprate OwnedPartition.
        """
        for broker, owned_partitions in self._partitions_by_leader.iteritems():
            reqs = []
            for owned_partition in owned_partitions:
                # attempt to acquire lock, just pass if we can't
                if owned_partition.lock.acquire(False):
                    has_room = owned_partition.message_count < self._queued_max_messages
                    if owned_partition.empty and has_room:
                        reqs.append(owned_partition.build_fetch_request(
                            self._fetch_message_max_bytes))
            if reqs:
                response = broker.fetch_messages(
                    reqs, timeout=self._fetch_wait_max_ms,
                    min_bytes=self._fetch_min_bytes
                )
                for partition_id, pres in response.topics[self._topic.name].iteritems():
                    owned_partition = self._partitions_by_id[partition_id]
                    owned_partition.enqueue_messages(pres.messages)
                    owned_partition.lock.release()


class OwnedPartition(object):
    """A partition that is owned by a SimpleConsumer.

    Used to keep track of offsets and the internal message queue.
    """

    def __init__(self,
                 partition,
                 consumer_group=None):
        self.partition = partition
        self._messages = Queue()
        self.last_offset_consumed = 0
        self.next_offset = 0
        self.lock = threading.Lock()

    @property
    def message_count(self):
        return self._messages.qsize()

    @property
    def empty(self):
        return self._messages.empty()

    def set_offset_counters(self, res):
        """Set the internal offset counters from an OffsetFetchResponse

        :param res: an OffsetFetchPartitionResponse containing the committed
            offset for this partition
        :type res: <protocol.PartitionOffsetFetchResponse>
        """
        self.last_offset_consumed = res.offset
        self.next_offset = res.offset + 1
        log.info("Last offset consumed: %d", self.last_offset_consumed)

    def build_fetch_request(self, max_bytes):
        return PartitionFetchRequest(
            self.partition.topic.name, self.partition.id,
            self.next_offset, max_bytes)

    def build_offset_commit_request(self):
        """Create a PartitionOffsetCommitRequest for this partition
        """
        return PartitionOffsetCommitRequest(
            self.partition.topic.name,
            self.partition.id,
            self.last_offset_consumed,
            int(time.time()),
            ''  # TODO - what to do with metadata?
        )

    def build_offset_fetch_request(self):
        """Create a PartitionOffsetFetchRequest for this partition
        """
        return PartitionOffsetFetchRequest(
            self.partition.topic.name,
            self.partition.id
        )

    def consume(self):
        """Get a single message from this partition
        """
        try:
            message = self._messages.get_nowait()
            self.last_offset_consumed = message.offset
            return message
        except Empty:
            return None

    def enqueue_messages(self, messages):
        for message in messages:
            if message.offset < self.last_offset_consumed:
                continue
            self._messages.put(message)
            self.next_offset = message.offset + 1
