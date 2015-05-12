from __future__ import division
"""
Author: Emmett Butler
"""
__license__ = """
Copyright 2015 Parse.ly, Inc.

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
__all__ = ["SimpleConsumer"]
import itertools
import logging as log
import time
import threading
from collections import defaultdict
from Queue import Queue, Empty

import base
from .common import OffsetType
from .exceptions import (OffsetOutOfRangeError, UnknownTopicOrPartition,
                         OffsetMetadataTooLarge, OffsetsLoadInProgress,
                         NotCoordinatorForConsumer, ERROR_CODES)
from .protocol import (PartitionFetchRequest, PartitionOffsetCommitRequest,
                       PartitionOffsetFetchRequest, PartitionOffsetRequest)
from .utils.error_handlers import handle_partition_responses, raise_error


class SimpleConsumer(base.BaseSimpleConsumer):
    """
    A non-balancing consumer for Kafka
    """
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
                 consumer_timeout_ms=-1,
                 auto_start=True,
                 reset_offset_on_start=False):
        """Create a SimpleConsumer.

        Settings and default values are taken from the Scala
        consumer implementation.  Consumer group is included
        because it's necessary for offset management, but doesn't imply
        that this is a balancing consumer. Use a BalancedConsumer for
        that.

        :param topic: The topic this consumer should consume
        :type topic: :class:`pykafka.topic.Topic`
        :param cluster: The cluster to which this consumer should connect
        :type cluster: :class:`pykafka.cluster.Cluster`
        :param consumer_group: The name of the consumer group to join
        :type consumer_group: str
        :param partitions: Existing partitions to which to connect
        :type partitions: Iterable of :class:`pykafka.partition.Partition`
        :param fetch_message_max_bytes: The number of bytes of messages to
            attempt to fetch
        :type fetch_message_max_bytes: int
        :param num_consumer_fetchers: The number of workers used to fetch data
        :type num_consumer_fetchers: int
        :param auto_commit_enable: If true, periodically commit to kafka the
            offset of messages already fetched by this consumer.
        :type auto_commit_enable: bool
        :param auto_commit_interval_ms: The frequency (in milliseconds) at which the
            consumer offsets are committed to kafka
        :type auto_commit_interval_ms: int
        :param queued_max_messages: Maximum number of messages buffered for
            consumption
        :type queued_max_messages: int
        :param fetch_min_bytes: The minimum amount of data (in bytes) the server
            should return for a fetch request. If insufficient data is available
            the request will block.
        :type fetch_min_bytes: int
        :param fetch_wait_max_ms: The maximum amount of time (in milliseconds)
            the server will block before answering the fetch request if there
            isn't sufficient data to immediately satisfy fetch_min_bytes.
        :type fetch_wait_max_ms: int
        :param refresh_leader_backoff_ms: Backoff time (in milliseconds) to
            refresh the leader of a partition after it loses the current leader.
        :type refresh_leader_backoff_ms: int
        :param offsets_channel_backoff_ms: Backoff time (in milliseconds) to
            retry offset commits/fetches
        :type offsets_channel_backoff_ms: int
        :param offsets_commit_max_retries: Retry the offset commit up to this
            many times on failure.
        :type offsets_commit_max_retries: int
        :param auto_offset_reset: What to do if an offset is out of range. This
            setting indicates how to reset the consumer's internal offset
            counter when an OffsetOutOfRangeError is encountered.
        :type auto_offset_reset: :class:`pykafka.common.OffsetType`
        :param consumer_timeout_ms: Amount of time (in milliseconds) the
            consumer may spend without messages available for consumption
            before raising an error.
        :type consumer_timeout_ms: int
        :param auto_start: Whether the consumer should begin communicating
            with kafka after __init__ is complete. If false, communication
            can be started with `start()`.
        :type auto_start: bool
        :param reset_offset_on_start: Whether the consumer should reset its
            internal offset counter to `self._auto_offset_reset` immediately
            upon starting up
        :type reset_offset_on_start: bool
        """
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
        self._auto_start = auto_start
        self._reset_offset_on_start = reset_offset_on_start

        self._last_message_time = time.time()

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._last_auto_commit = time.time()

        self._discover_offset_manager()

        if partitions:
            self._partitions = {OwnedPartition(p): p
                                for p in partitions}
        else:
            self._partitions = {OwnedPartition(p): topic.partitions[k]
                                for k, p in topic.partitions.iteritems()}
        self._partitions_by_id = {p.partition.id: p
                                  for p in self._partitions.iterkeys()}
        # Organize partitions by leader for efficient queries
        self._partitions_by_leader = defaultdict(list)
        for p in self._partitions.iterkeys():
            self._partitions_by_leader[p.partition.leader].append(p)
        self.partition_cycle = itertools.cycle(self._partitions.keys())

        self._default_error_handlers = self._build_default_error_handlers()

        if self._auto_start:
            self.start()

    def __repr__(self):
        return "<{}.{} at {} (consumer_group={})>".format(
            self.__class__.__module__,
            self.__class__.__name__,
            hex(id(self)),
            self._consumer_group
        )

    def start(self):
        """Begin communicating with Kafka, including setting up worker threads

        Fetches offsets, starts an offset autocommitter worker pool, and
        starts a message fetcher worker pool.
        """
        self._running = True

        if self._auto_commit_enable:
            self._autocommit_worker_thread = self._setup_autocommit_worker()

        # Figure out which offset wer're starting on
        if self._reset_offset_on_start:
            self._reset_offsets()
        elif self._consumer_group is not None:
            self.fetch_offsets()

        self._fetch_workers = self._setup_fetch_workers()

    def _build_default_error_handlers(self):
        """Set up the error handlers to use for partition errors."""
        def _handle_OffsetOutOfRangeError(parts):
            self._reset_offsets([owned_partition
                                 for owned_partition, pres in parts])

        def _handle_NotCoordinatorForConsumer(parts):
            self._discover_offset_manager()

        return {
            UnknownTopicOrPartition.ERROR_CODE: lambda p: raise_error(UnknownTopicOrPartition),
            OffsetOutOfRangeError.ERROR_CODE: _handle_OffsetOutOfRangeError,
            OffsetMetadataTooLarge.ERROR_CODE: lambda p: raise_error(OffsetMetadataTooLarge),
            NotCoordinatorForConsumer.ERROR_CODE: _handle_NotCoordinatorForConsumer
        }

    def _discover_offset_manager(self):
        """Set the offset manager for this consumer.

        If a consumer group is not supplied to __init__, this method does nothing
        """
        if self._consumer_group is not None:
            self._offset_manager = self._cluster.get_offset_manager(self._consumer_group)

    @property
    def topic(self):
        """The topic this consumer consumes"""
        return self._topic

    @property
    def partitions(self):
        """A list of the partitions that this consumer consumes"""
        return self._partitions

    def __del__(self):
        """Stop consumption and workers when object is deleted"""
        self.stop()

    def stop(self):
        """Flag all running workers for deletion."""
        self._running = False

    def _setup_autocommit_worker(self):
        """Start the autocommitter thread"""
        def autocommitter():
            while True:
                if not self._running:
                    break
                if self._auto_commit_enable:
                    self._auto_commit()
                time.sleep(self._auto_commit_interval_ms / 1000)
            log.debug("Autocommitter thread exiting")
        log.debug("Starting autocommitter thread")
        return self._cluster.handler.spawn(autocommitter)

    def _setup_fetch_workers(self):
        """Start the fetcher threads"""
        def fetcher():
            while True:
                if not self._running:
                    break
                self.fetch()
                time.sleep(.0001)
            log.debug("Fetcher thread exiting")
        log.info("Starting %s fetcher threads", self._num_consumer_fetchers)
        return [self._cluster.handler.spawn(fetcher)
                for i in xrange(self._num_consumer_fetchers)]

    def __iter__(self):
        """Yield an infinite stream of messages until the consumer times out"""
        while True:
            message = self.consume(block=False)
            if not message and self._consumer_timed_out():
                raise StopIteration
            yield message

    def _consumer_timed_out(self):
        """Indicates whether the consumer has received messages recently"""
        if self._consumer_timeout_ms == -1:
            return False
        disp = (time.time() - self._last_message_time) * 1000.0
        return disp > self._consumer_timeout_ms

    def consume(self, block=True):
        """Get one message from the consumer.

        :param block: Whether to block while waiting for a message
        :type block: bool
        """
        self._last_message_time = time.time()
        message = None
        while not message and not self._consumer_timed_out():
            owned_partition = self.partition_cycle.next()
            message = owned_partition.consume()

            if message:
                self._last_message_time = time.time()
                return message
            if not block:
                break

    def _auto_commit(self):
        """Commit offsets only if it's time to do so"""
        if not self._auto_commit_enable or self._auto_commit_interval_ms == 0:
            return

        if (time.time() - self._last_auto_commit) * 1000.0 >= self._auto_commit_interval_ms:
            log.info("Autocommitting consumer offset for consumer group %s and topic %s",
                     self._consumer_group, self._topic.name)
            self.commit_offsets()
            self._last_auto_commit = time.time()

    def commit_offsets(self):
        """Commit offsets for this consumer's partitions

        Uses the offset commit/fetch API
        """
        if not self._consumer_group:
            raise Exception("consumer group must be specified to commit offsets")

        reqs = [p.build_offset_commit_request() for p in self._partitions.keys()]
        log.info("Committing offsets for %d partitions to broker id %s", len(reqs),
                 self._offset_manager.id)
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
            log.error("Error committing offsets for topic %s (errors: %s)",
                      self._topic.name,
                      {ERROR_CODES[err]: [op.partition.id for op, _ in parts]
                       for err, parts in parts_by_error.iteritems()})

            # retry only the partitions that errored
            if 0 in parts_by_error:
                parts_by_error.pop(0)
            errored_partitions = [op for code, err_group in parts_by_error.iteritems()
                                  for op, res in err_group]
            reqs = [p.build_offset_commit_request() for p in errored_partitions]

    def fetch_offsets(self):
        """Fetch offsets for this consumer's topic

        Uses the offset commit/fetch API

        :return: List of (id, :class:`pykafka.protocol.OffsetFetchPartitionResponse`)
            tuples
        """
        if not self._consumer_group:
            raise Exception("consumer group must be specified to fetch offsets")

        def _handle_success(parts):
            for owned_partition, pres in parts:
                owned_partition.set_offset(pres.offset)

        log.info("Fetching offsets")

        reqs = [p.build_offset_fetch_request() for p in self._partitions.keys()]
        success_responses = []

        for i in xrange(self._offsets_fetch_max_retries):
            if i > 0:
                log.info("Retrying")

            res = self._offset_manager.fetch_consumer_group_offsets(self._consumer_group, reqs)
            parts_by_error = handle_partition_responses(
                res,
                self._default_error_handlers,
                success_handler=_handle_success,
                partitions_by_id=self._partitions_by_id)

            success_responses.extend([(op.partition.id, r)
                                      for op, r in parts_by_error.get(0, [])])
            if len(parts_by_error) == 1 and 0 in parts_by_error:
                return success_responses
            log.error("Error fetching offsets for topic %s (errors: %s)",
                      self._topic.name,
                      {ERROR_CODES[err]: [op.partition.id for op, _ in parts]
                       for err, parts in parts_by_error.iteritems()})

            # retry only specific error responses
            to_retry = []
            to_retry.extend(parts_by_error.get(OffsetsLoadInProgress.ERROR_CODE, []))
            to_retry.extend(parts_by_error.get(NotCoordinatorForConsumer.ERROR_CODE, []))
            reqs = [p.build_offset_fetch_request() for p, _ in to_retry]

    def _reset_offsets(self, partitions=None):
        """Reset offsets after an error

        Issue an OffsetRequest for each partition and set the appropriate
        returned offset in the OwnedPartition per self._auto_offset_reset

        :param partitions: the partitions for which to reset offsets
        :type partitions: Iterable of
            :class:`pykafka.simpleconsumer.OwnedPartition`
        """
        def _handle_success(parts):
            for owned_partition, pres in parts:
                owned_partition.set_offset(pres.offset[0])

        if partitions is None:
            partitions = self._partitions.keys()

        log.info("Resetting offsets for %s partitions", len(list(partitions)))

        # group partitions by leader
        by_leader = defaultdict(list)
        for p in partitions:
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
                log.debug("Fetched %s messages for partition %s",
                          len(pres.messages), owned_partition.partition.id)
                owned_partition.enqueue_messages(pres.messages)

        for broker, owned_partitions in self._partitions_by_leader.iteritems():
            partition_reqs = {}
            for owned_partition in owned_partitions:
                # attempt to acquire lock, just pass if we can't
                if owned_partition.lock.acquire(False):
                    partition_reqs[owned_partition] = None
                    if owned_partition.message_count < self._queued_max_messages:
                        fetch_req = owned_partition.build_fetch_request(
                            self._fetch_message_max_bytes)
                        partition_reqs[owned_partition] = fetch_req
                    else:
                        log.debug("Partition %s above max queued count (queue has %d)",
                                  owned_partition.partition.id,
                                  owned_partition.message_count)
            if partition_reqs:
                response = broker.fetch_messages(
                    [a for a in partition_reqs.itervalues() if a],
                    timeout=self._fetch_wait_max_ms,
                    min_bytes=self._fetch_min_bytes
                )
                handle_partition_responses(
                    response,
                    self._default_error_handlers,
                    success_handler=_handle_success,
                    partitions_by_id=self._partitions_by_id)
            for owned_partition in partition_reqs.iterkeys():
                owned_partition.lock.release()


class OwnedPartition(object):
    """A partition that is owned by a SimpleConsumer.

    Used to keep track of offsets and the internal message queue.
    """

    def __init__(self,
                 partition):
        """
        :param partition: The partition to hold
        :type partition: :class:`pykafka.partition.Partition`
        """
        self.partition = partition
        self._messages = Queue()
        self.last_offset_consumed = 0
        self.next_offset = 0
        self.lock = threading.Lock()

    @property
    def message_count(self):
        """Count of messages currently in this partition's internal queue"""
        return self._messages.qsize()

    def set_offset(self, last_offset_consumed):
        """Set the internal offset counters

        :param last_offset_consumed: The last committed offset for this
            partition
        :type last_offset_consumed: int
        """
        self.last_offset_consumed = last_offset_consumed
        self.next_offset = last_offset_consumed + 1

    def build_offset_request(self, auto_offset_reset):
        """Create a :class:`pykafka.protocol.PartitionOffsetRequest` for this
            partition

        :param auto_offset_reset: What to do if an offset is out of range. This
            setting indicates how to reset the consumer's internal offset
            counter when an OffsetOutOfRangeError is encountered.
        :type auto_offset_reset: :class:`pykafka.common.OffsetType`
        """
        return PartitionOffsetRequest(
            self.partition.topic.name, self.partition.id,
            auto_offset_reset, 1)

    def build_fetch_request(self, max_bytes):
        """Create a :class:`pykafka.protocol.FetchPartitionRequest` for this
            partition.

        :param max_bytes: The number of bytes of messages to
            attempt to fetch
        :type max_bytes: int
        """
        return PartitionFetchRequest(
            self.partition.topic.name, self.partition.id,
            self.next_offset, max_bytes)

    def build_offset_commit_request(self):
        """Create a :class:`pykafka.protocol.PartitionOffsetCommitRequest`
            for this partition
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
        """Get a single message from this partition"""
        try:
            message = self._messages.get_nowait()
            self.last_offset_consumed = message.offset
            return message
        except Empty:
            return None

    def enqueue_messages(self, messages):
        """Put a set of messages into the internal message queue

        :param messages: The messages to enqueue
        :type messages: Iterable of :class:`pykafka.common.Message`
        """
        for message in messages:
            if message.offset < self.last_offset_consumed:
                continue
            self._messages.put(message)
            self.next_offset = message.offset + 1
