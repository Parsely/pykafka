import functools
import itertools
from collections import defaultdict
import time
import logging as log
from Queue import Queue, Empty
import weakref
import threading

import base
from .common import OffsetType
from .exceptions import (OffsetOutOfRangeError, UnknownTopicOrPartition,
                                OffsetMetadataTooLarge, OffsetsLoadInProgress,
                                NotCoordinatorForConsumer)

from .utils.error_handlers import handle_partition_responses, raise_error
from .protocol import (PartitionFetchRequest, PartitionOffsetCommitRequest,
                       PartitionOffsetFetchRequest, PartitionOffsetRequest)


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
        if not isinstance(cluster, weakref.ProxyType) and \
                not isinstance(cluster, weakref.CallableProxyType):
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
        self._offsets_channel_backoff_ms = offsets_channel_backoff_ms
        self._auto_offset_reset = auto_offset_reset
        self._offsets_commit_max_retries = offsets_commit_max_retries
        # not directly configurable
        self._offsets_fetch_max_retries = offsets_commit_max_retries

        self._last_message_time = time.time()

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._last_auto_commit = time.time()

        self._discover_offset_manager()

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

        self._default_error_handlers = self._build_default_error_handlers()

        self._running = True

        if self._auto_commit_enable:
            self._autocommit_worker_thread = self._setup_autocommit_worker()
        # we need to get the most up-to-date offsets before starting consumption
        self.fetch_offsets()
        self._fetch_workers = self._setup_fetch_workers()

    def _build_default_error_handlers(self):
        def _handle_OffsetOutOfRangeError(parts):
            self._reset_offsets((owned_partition
                                 for owned_partition, pres in parts))

        def _handle_NotCoordinatorForConsumer(parts):
            self._discover_offset_manager()

        return {
            UnknownTopicOrPartition.ERROR_CODE: lambda p: raise_error(UnknownTopicOrPartition),
            OffsetOutOfRangeError.ERROR_CODE: _handle_OffsetOutOfRangeError,
            OffsetMetadataTooLarge.ERROR_CODE: lambda p: raise_error(OffsetMetadataTooLarge),
            NotCoordinatorForConsumer.ERROR_CODE: _handle_NotCoordinatorForConsumer
        }

    def _discover_offset_manager(self):
        self._offset_manager = self._cluster.get_offset_manager(self._consumer_group)

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

        reqs = [p.build_offset_commit_request() for p in self._partitions.keys()]
        log.info("Committing offsets for %d partitions", len(reqs))
        for i in xrange(self._offsets_commit_max_retries):
            if i > 0:
                log.debug("Retrying")
            time.sleep(i * (self._offsets_channel_backoff_ms / 1000))

            response = self._offset_manager.commit_consumer_group_offsets(
                self._consumer_group, 1, 'pykafka', reqs)
            parts_by_error = handle_partition_responses(
                response,
                self._default_error_handlers,
                partitions_by_id=self._partitions_by_id)
            if len(parts_by_error) == 1 and 0 in parts_by_error:
                break
            log.error("Error committing offsets for topic %s", self._topic.name)

            # retry only the partitions that errored
            parts_by_error.pop(0)
            errored_partitions = [op for err_group in parts_by_error.iteritems() for op in err_group]
            reqs = [p.build_offset_commit_request() for p in errored_partitions]

    def fetch_offsets(self):
        """Fetch offsets for this consumer's topic

        Uses the offset commit/fetch API
        Should be called when consumer starts and after any errors
        """
        if not self._consumer_group:
            raise Exception("consumer group must be specified to fetch offsets")

        def _handle_success(parts):
            for owned_partition, pres in parts:
                owned_partition.set_offset(pres.offset)

        log.info("Fetching offsets")

        reqs = [p.build_offset_fetch_request() for p in self._partitions.keys()]

        for i in xrange(self._offsets_fetch_max_retries):
            if i > 0:
                log.debug("Retrying")

            res = self._offset_manager.fetch_consumer_group_offsets(self._consumer_group, reqs)
            parts_by_error = handle_partition_responses(
                res,
                self._default_error_handlers,
                success_handler=_handle_success,
                partitions_by_id=self._partitions_by_id)

            if len(parts_by_error) == 1 and 0 in parts_by_error:
                break
            log.error("Error fetching offsets for topic %s", self._topic.name)

            # retry only OffsetsLoadInProgress responses
            reqs = [p.build_offset_fetch_request()
                    for p in parts_by_error.get(OffsetsLoadInProgress.ERROR_CODE, [])]

    def _reset_offsets(self, errored_partitions):
        """Reset offsets after an OffsetOutOfRangeError

        Issue an OffsetRequest for each partition and set the appropriate
        returned offset in the OwnedPartition per self._auto_offset_reset

        :param errored_partitions: the partitions with out-of-range offsets
        :type errored_partitions: Iterable of OwnedPartition
        """
        def _handle_success(parts):
            for owned_partition, pres in parts:
                owned_partition.set_offset(pres.offset[0])

        # group out-of-range partitions by leader
        by_leader = defaultdict(list)
        for p in errored_partitions:
            by_leader[p.partition.leader].append(p)

        # get valid offset ranges for each partition
        for broker, owned_partitions in by_leader.iteritems():
            reqs = [owned_partition.build_offset_request(self._auto_offset_reset)
                    for owned_partition in owned_partitions]
            response = broker.request_offset_limits(reqs)
            handle_partition_responses(
                response,
                self._default_error_handlers,
                success_handler=_handle_success,
                partitions_by_id=self._partitions_by_id)

    def fetch(self):
        """Fetch new messages for all partitions

        Create a FetchRequest for each broker and send it. Enqueue each of the
        returned messages in the approprate OwnedPartition.
        """
        def _handle_success(parts):
            for owned_partition, pres in parts:
                owned_partition.enqueue_messages(pres.messages)

        for broker, owned_partitions in self._partitions_by_leader.iteritems():
            partition_reqs = []
            for owned_partition in owned_partitions:
                # attempt to acquire lock, just pass if we can't
                if owned_partition.lock.acquire(False) and \
                        owned_partition.message_count < self._queued_max_messages:
                    fetch_req = owned_partition.build_fetch_request(
                        self._fetch_message_max_bytes)
                    partition_reqs.append((owned_partition, fetch_req))
            if partition_reqs:
                response = broker.fetch_messages(
                    [a[1] for a in partition_reqs],
                    timeout=self._fetch_wait_max_ms,
                    min_bytes=self._fetch_min_bytes
                )
                handle_partition_responses(
                    response,
                    self._default_error_handlers,
                    success_handler=_handle_success,
                    partitions_by_id=self._partitions_by_id)
            for owned_partition, _ in partition_reqs:
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

    def set_offset(self, last_offset_consumed):
        """Set the internal offset counters from an OffsetFetchResponse

        :param last_offset_consumed: the last committed offset for this
            partition
        :type last_offset_consumed: int
        """
        self.last_offset_consumed = last_offset_consumed
        self.next_offset = last_offset_consumed + 1

    def build_offset_request(self, auto_offset_reset):
        """Create a PartitionOffsetRequest for this partition
        """
        return PartitionOffsetRequest(
            self.partition.topic.name, self.partition.id,
            auto_offset_reset, 1)

    def build_fetch_request(self, max_bytes):
        """Create a FetchPartitionRequest for this partition
        """
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
            'pykafka'
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
