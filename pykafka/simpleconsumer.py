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
import logging
import time
import threading
from collections import defaultdict

from .common import OffsetType
from .utils.compat import (Semaphore, Queue, Empty, iteritems, itervalues,
                           range, iterkeys)
from .exceptions import (OffsetOutOfRangeError, UnknownTopicOrPartition,
                         OffsetMetadataTooLarge, OffsetsLoadInProgress,
                         NotCoordinatorForConsumer, SocketDisconnectedError,
                         ConsumerStoppedException, KafkaException,
                         OffsetRequestFailedError, ERROR_CODES)
from .protocol import (PartitionFetchRequest, PartitionOffsetCommitRequest,
                       PartitionOffsetFetchRequest, PartitionOffsetRequest)
from .utils.error_handlers import (handle_partition_responses, raise_error,
                                   build_parts_by_error)


log = logging.getLogger(__name__)


class SimpleConsumer():
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
                 offsets_channel_backoff_ms=1000,
                 offsets_commit_max_retries=5,
                 auto_offset_reset=OffsetType.EARLIEST,
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
        :param consumer_group: The name of the consumer group this consumer
            should use for offset committing and fetching.
        :type consumer_group: bytes
        :param partitions: Existing partitions to which to connect
        :type partitions: Iterable of :class:`pykafka.partition.Partition`
        :param fetch_message_max_bytes: The number of bytes of messages to
            attempt to fetch
        :type fetch_message_max_bytes: int
        :param num_consumer_fetchers: The number of workers used to make
            FetchRequests
        :type num_consumer_fetchers: int
        :param auto_commit_enable: If true, periodically commit to kafka the
            offset of messages already fetched by this consumer. This also
            requires that `consumer_group` is not `None`.
        :type auto_commit_enable: bool
        :param auto_commit_interval_ms: The frequency (in milliseconds) at which the
            consumer offsets are committed to kafka. This setting is ignored if
            `auto_commit_enable` is `False`.
        :type auto_commit_interval_ms: int
        :param queued_max_messages: Maximum number of messages buffered for
            consumption
        :type queued_max_messages: int
        :param fetch_min_bytes: The minimum amount of data (in bytes) the server
            should return for a fetch request. If insufficient data is available
            the request will block until sufficient data is available.
        :type fetch_min_bytes: int
        :param fetch_wait_max_ms: The maximum amount of time (in milliseconds)
            the server will block before answering the fetch request if there
            isn't sufficient data to immediately satisfy `fetch_min_bytes`.
        :type fetch_wait_max_ms: int
        :param offsets_channel_backoff_ms: Backoff time (in milliseconds) to
            retry offset commits/fetches
        :type offsets_channel_backoff_ms: int
        :param offsets_commit_max_retries: Retry the offset commit up to this
            many times on failure.
        :type offsets_commit_max_retries: int
        :param auto_offset_reset: What to do if an offset is out of range. This
            setting indicates how to reset the consumer's internal offset
            counter when an `OffsetOutOfRangeError` is encountered.
        :type auto_offset_reset: :class:`pykafka.common.OffsetType`
        :param consumer_timeout_ms: Amount of time (in milliseconds) the
            consumer may spend without messages available for consumption
            before returning None.
        :type consumer_timeout_ms: int
        :param auto_start: Whether the consumer should begin communicating
            with kafka after __init__ is complete. If false, communication
            can be started with `start()`.
        :type auto_start: bool
        :param reset_offset_on_start: Whether the consumer should reset its
            internal offset counter to `self._auto_offset_reset` and commit that
            offset immediately upon starting up
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
        self._offsets_reset_max_retries = offsets_commit_max_retries
        self._auto_start = auto_start
        self._reset_offset_on_start = reset_offset_on_start

        # incremented for any message arrival from any partition
        # the initial value is 0 (no messages waiting)
        self._messages_arrived = Semaphore(value=0)

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._last_auto_commit = time.time()

        self._discover_offset_manager()

        if partitions:
            self._partitions = {p: OwnedPartition(p, self._messages_arrived)
                                for p in partitions}
        else:
            self._partitions = {topic.partitions[k]:
                                OwnedPartition(p, self._messages_arrived)
                                for k, p in iteritems(topic.partitions)}
        self._partitions_by_id = {p.partition.id: p
                                  for p in itervalues(self._partitions)}
        # Organize partitions by leader for efficient queries
        self._partitions_by_leader = defaultdict(list)
        for p in itervalues(self._partitions):
            self._partitions_by_leader[p.partition.leader].append(p)
        self.partition_cycle = itertools.cycle(self._partitions.values())

        self._default_error_handlers = self._build_default_error_handlers()

        self._running = False
        if self._auto_start:
            self.start()

    def __repr__(self):
        return "<{module}.{name} at {id_} (consumer_group={group})>".format(
            module=self.__class__.__module__,
            name=self.__class__.__name__,
            id_=hex(id(self)),
            group=self._consumer_group
        )

    def start(self):
        """Begin communicating with Kafka, including setting up worker threads

        Fetches offsets, starts an offset autocommitter worker pool, and
        starts a message fetcher worker pool.
        """
        self._running = True

        # Figure out which offset wer're starting on
        if self._reset_offset_on_start:
            self.reset_offsets()
        elif self._consumer_group is not None:
            self.fetch_offsets()

        self._fetch_workers = self._setup_fetch_workers()

        if self._auto_commit_enable:
            self._autocommit_worker_thread = self._setup_autocommit_worker()

    def _build_default_error_handlers(self):
        """Set up the error handlers to use for partition errors."""
        def _handle_OffsetOutOfRangeError(parts):
            log.info("Resetting offsets in response to OffsetOutOfRangeError")
            self.reset_offsets(
                partition_offsets=[(owned_partition.partition, self._auto_offset_reset)
                                   for owned_partition, pres in parts]
            )

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
        return {id_: partition.partition
                for id_, partition in iteritems(self._partitions_by_id)}

    @property
    def held_offsets(self):
        """Return a map from partition id to held offset for each partition"""
        return {p.partition.id: p.last_offset_consumed
                for p in itervalues(self._partitions_by_id)}

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
                for i in range(self._num_consumer_fetchers)]

    def __iter__(self):
        """Yield an infinite stream of messages until the consumer times out"""
        while True:
            message = self.consume(block=True)
            if not message:
                raise StopIteration
            yield message

    def consume(self, block=True):
        """Get one message from the consumer.

        :param block: Whether to block while waiting for a message
        :type block: bool
        """
        timeout = None
        if block:
            if self._consumer_timeout_ms > 0:
                timeout = float(self._consumer_timeout_ms) / 1000
            else:
                timeout = 1.0

        while True:
            if self._messages_arrived.acquire(blocking=block, timeout=timeout):
                # by passing through this semaphore, we know that at
                # least one message is waiting in some queue.
                message = None
                while not message:
                    owned_partition = next(self.partition_cycle)
                    message = owned_partition.consume()
                return message
            else:
                if not self._running:
                    raise ConsumerStoppedException()
                elif not block or self._consumer_timeout_ms > 0:
                    return None

    def _auto_commit(self):
        """Commit offsets only if it's time to do so"""
        if not self._auto_commit_enable or self._auto_commit_interval_ms == 0:
            return

        if (time.time() - self._last_auto_commit) * 1000.0 >= self._auto_commit_interval_ms:
            log.info("Autocommitting consumer offset for consumer group %s and topic %s",
                     self._consumer_group, self._topic.name)
            if self._consumer_group is not None:
                self.commit_offsets()
            self._last_auto_commit = time.time()

    def commit_offsets(self):
        """Commit offsets for this consumer's partitions

        Uses the offset commit/fetch API
        """
        if not self._consumer_group:
            raise Exception("consumer group must be specified to commit offsets")

        reqs = [p.build_offset_commit_request() for p in self._partitions.values()]
        log.debug("Committing offsets for %d partitions to broker id %s", len(reqs),
                  self._offset_manager.id)
        for i in range(self._offsets_commit_max_retries):
            if i > 0:
                log.debug("Retrying")
            time.sleep(i * (self._offsets_channel_backoff_ms / 1000))

            response = self._offset_manager.commit_consumer_group_offsets(
                self._consumer_group, 1, b'pykafka', reqs)
            parts_by_error = handle_partition_responses(
                self._default_error_handlers,
                response=response,
                partitions_by_id=self._partitions_by_id)
            if len(parts_by_error) == 1 and 0 in parts_by_error:
                break
            log.error("Error committing offsets for topic %s (errors: %s)",
                      self._topic.name,
                      {ERROR_CODES[err]: [op.partition.id for op, _ in parts]
                       for err, parts in iteritems(parts_by_error)})

            # retry only the partitions that errored
            if 0 in parts_by_error:
                parts_by_error.pop(0)
            errored_partitions = [
                op for code, err_group in iteritems(parts_by_error)
                for op, res in err_group
            ]
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
            partition_offsets_to_reset = []
            for owned_partition, pres in parts:
                # If Kafka returned -1, that means that no
                # offset was associated with this consumer group.
                # This partition will have its offset reset.
                if pres.offset == -1:
                    log.debug("Partition %s has no committed offsets in "
                              "consumer group %s.  Resetting to %s",
                              owned_partition.partition.id,
                              self._consumer_group,
                              self._auto_offset_reset)
                    partition_offsets_to_reset.append((
                        owned_partition.partition,
                        self._auto_offset_reset
                    ))
                else:
                    log.debug("Set offset for partition %s to %s",
                              owned_partition.partition.id,
                              pres.offset)
                    owned_partition.set_offset(pres.offset)

            # If any partitions didn't have a committed offset,
            # then reset those partition's offsets.
            if partition_offsets_to_reset:
                self.reset_offsets(partition_offsets_to_reset)

        reqs = [p.build_offset_fetch_request() for p in self._partitions.values()]
        success_responses = []

        log.debug("Fetching offsets for %d partitions from broker id %s", len(reqs),
                  self._offset_manager.id)

        for i in range(self._offsets_fetch_max_retries):
            if i > 0:
                log.debug("Retrying offset fetch")

            res = self._offset_manager.fetch_consumer_group_offsets(self._consumer_group, reqs)
            parts_by_error = handle_partition_responses(
                self._default_error_handlers,
                response=res,
                success_handler=_handle_success,
                partitions_by_id=self._partitions_by_id)

            success_responses.extend([(op.partition.id, r)
                                      for op, r in parts_by_error.get(0, [])])
            if len(parts_by_error) == 1 and 0 in parts_by_error:
                return success_responses
            log.error("Error fetching offsets for topic %s (errors: %s)",
                      self._topic.name,
                      {ERROR_CODES[err]: [op.partition.id for op, _ in parts]
                       for err, parts in iteritems(parts_by_error)})

            time.sleep(i * (self._offsets_channel_backoff_ms / 1000))

            # retry only specific error responses
            to_retry = []
            to_retry.extend(parts_by_error.get(OffsetsLoadInProgress.ERROR_CODE, []))
            to_retry.extend(parts_by_error.get(NotCoordinatorForConsumer.ERROR_CODE, []))
            reqs = [p.build_offset_fetch_request() for p, _ in to_retry]

    def reset_offsets(self, partition_offsets=None):
        """Reset offsets for the specified partitions

        Issue an OffsetRequest for each partition and set the appropriate
        returned offset in the consumer's internal offset counter.

        :param partition_offsets: (`partition`, `timestamp_or_offset`) pairs to
            reset where `partition` is the partition for which to reset the offset
            and `timestamp_or_offset` is EITHER the timestamp of the message
            whose offset the partition should have OR the new offset the
            partition should have
        :type partition_offsets: Iterable of
            (:class:`pykafka.partition.Partition`, int)

        NOTE: If an instance of `timestamp_or_offset` is treated by kafka as
        an invalid offset timestamp, this function directly sets the consumer's
        internal offset counter for that partition to that instance of
        `timestamp_or_offset`. On the next fetch request, the consumer attempts
        to fetch messages starting from that offset. See the following link
        for more information on what kafka treats as a valid offset timestamp:
        https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetRequest
        """
        def _handle_success(parts):
            for owned_partition, pres in parts:
                if len(pres.offset) > 0:
                    # offset requests return the next offset to consume,
                    # so account for this here by passing offset - 1
                    owned_partition.set_offset(pres.offset[0] - 1)
                else:
                    # If the number specified in partition_offsets is an invalid
                    # timestamp value for the partition, kafka does the
                    # following:
                    #   returns an empty array in pres.offset
                    #   returns error code 0
                    # Here, we detect this case and set the consumer's internal
                    # offset to that value. Thus, the next fetch request will
                    # attempt to fetch from that offset. If it succeeds, all is
                    # well; if not, reset_offsets is called again by the error
                    # handlers in fetch() and fetching continues from
                    # self._auto_offset_reset..
                    # This amounts to a hacky way to support user-specified
                    # offsets in reset_offsets by working around a bug or bad
                    # design decision in kafka.
                    given_offset = owned_partition_offsets[owned_partition]
                    log.warning(
                        "Offset reset for partition {id_} to timestamp {offset}"
                        " failed. Setting partition {id_}'s internal counter"
                        " to {offset}".format(
                            id_=owned_partition.partition.id, offset=given_offset))
                    owned_partition.set_offset(given_offset)
                # release locks on succeeded partitions to allow fetching
                # to resume
                owned_partition.fetch_lock.release()

        if partition_offsets is None:
            partition_offsets = [(a, self._auto_offset_reset)
                                 for a in self._partitions.keys()]

        # turn Partitions into their corresponding OwnedPartitions
        try:
            owned_partition_offsets = {self._partitions[p]: offset
                                       for p, offset in partition_offsets}
        except KeyError as e:
            raise KafkaException("Unknown partition supplied to reset_offsets\n%s", e)

        log.info("Resetting offsets for %s partitions", len(list(owned_partition_offsets)))

        for i in range(self._offsets_reset_max_retries):
            # group partitions by leader
            by_leader = defaultdict(list)
            for partition, offset in iteritems(owned_partition_offsets):
                # acquire lock for each partition to stop fetching during offset
                # reset
                if partition.fetch_lock.acquire(True):
                    # empty the queue for this partition to avoid sending
                    # emitting messages from the old offset
                    partition.flush()
                    by_leader[partition.partition.leader].append((partition, offset))

            # get valid offset ranges for each partition
            for broker, offsets in iteritems(by_leader):
                reqs = [owned_partition.build_offset_request(offset)
                        for owned_partition, offset in offsets]
                response = broker.request_offset_limits(reqs)
                parts_by_error = handle_partition_responses(
                    self._default_error_handlers,
                    response=response,
                    success_handler=_handle_success,
                    partitions_by_id=self._partitions_by_id)

                if 0 in parts_by_error:
                    # drop successfully reset partitions for next retry
                    successful = [part for part, _ in parts_by_error.pop(0)]
                    # py3 creates a generate so we need to evaluate this
                    # operation
                    list(map(owned_partition_offsets.pop, successful))
                if not parts_by_error:
                    continue
                log.error("Error resetting offsets for topic %s (errors: %s)",
                          self._topic.name,
                          {ERROR_CODES[err]: [op.partition.id for op, _ in parts]
                           for err, parts in iteritems(parts_by_error)})

                time.sleep(i * (self._offsets_channel_backoff_ms / 1000))

                for errcode, owned_partitions in iteritems(parts_by_error):
                    if errcode != 0:
                        for owned_partition in owned_partitions:
                            owned_partition.fetch_lock.release()

            if not owned_partition_offsets:
                break
            log.debug("Retrying offset reset")

        if owned_partition_offsets:
            raise OffsetRequestFailedError("reset_offsets failed after %d "
                                           "retries",
                                           self._offsets_reset_max_retries)

        if self._consumer_group is not None:
            self.commit_offsets()

    def fetch(self):
        """Fetch new messages for all partitions

        Create a FetchRequest for each broker and send it. Enqueue each of the
        returned messages in the approprate OwnedPartition.
        """
        def _handle_success(parts):
            for owned_partition, pres in parts:
                if len(pres.messages) > 0:
                    log.debug("Fetched %s messages for partition %s",
                              len(pres.messages), owned_partition.partition.id)
                    owned_partition.enqueue_messages(pres.messages)
                    log.debug("Partition %s queue holds %s messages",
                              owned_partition.partition.id,
                              owned_partition.message_count)

        for broker, owned_partitions in iteritems(self._partitions_by_leader):
            partition_reqs = {}
            for owned_partition in owned_partitions:
                # attempt to acquire lock, just pass if we can't
                if owned_partition.fetch_lock.acquire(False):
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
                try:
                    response = broker.fetch_messages(
                        [a for a in itervalues(partition_reqs) if a],
                        timeout=self._fetch_wait_max_ms,
                        min_bytes=self._fetch_min_bytes
                    )
                except SocketDisconnectedError:
                    # If the broker dies while we're supposed to stop,
                    # it's fine, and probably an integration test.
                    if not self._running:
                        return
                    else:
                        raise

                parts_by_error = build_parts_by_error(response, self._partitions_by_id)
                # release the lock in these cases, since resolving the error
                # requires an offset reset and not releasing the lock would
                # lead to a deadlock in reset_offsets. For successful requests
                # or requests with different errors, we still assume that
                # it's ok to retain the lock since no offset_reset can happen
                # before this function returns
                out_of_range = parts_by_error.get(OffsetOutOfRangeError.ERROR_CODE, [])
                for owned_partition, res in out_of_range:
                    owned_partition.fetch_lock.release()
                    # remove them from the dict of partitions to unlock to avoid
                    # double-unlocking
                    partition_reqs.pop(owned_partition)
                # handle the rest of the errors that don't require deadlock
                # management
                handle_partition_responses(
                    self._default_error_handlers,
                    parts_by_error=parts_by_error,
                    success_handler=_handle_success)
                # unlock the rest of the partitions
                for owned_partition in iterkeys(partition_reqs):
                    owned_partition.fetch_lock.release()


class OwnedPartition(object):
    """A partition that is owned by a SimpleConsumer.

    Used to keep track of offsets and the internal message queue.
    """

    def __init__(self,
                 partition,
                 semaphore=None):
        """
        :param partition: The partition to hold
        :type partition: :class:`pykafka.partition.Partition`
        :param semaphore: A Semaphore that counts available messages and
            facilitates non-busy blocking
        :type semaphore: :class:`pykafka.utils.compat.Semaphore`
        """
        self.partition = partition
        self._messages = Queue()
        self._messages_arrived = semaphore
        self.last_offset_consumed = 0
        self.next_offset = 0
        self.fetch_lock = threading.Lock()

    @property
    def message_count(self):
        """Count of messages currently in this partition's internal queue"""
        return self._messages.qsize()

    def flush(self):
        """Flush internal queue"""
        # Swap out _messages so a concurrent consume/enqueue won't interfere
        tmp = self._messages
        self._messages = Queue()
        while True:
            try:
                tmp.get_nowait()
                self._messages_arrived.acquire(blocking=False)
            except Empty:
                break
        log.info("Flushed queue for partition %d", self.partition.id)

    def set_offset(self, last_offset_consumed):
        """Set the internal offset counters

        :param last_offset_consumed: The last committed offset for this
            partition
        :type last_offset_consumed: int
        """
        self.last_offset_consumed = last_offset_consumed
        self.next_offset = last_offset_consumed + 1

    def build_offset_request(self, new_offset):
        """Create a :class:`pykafka.protocol.PartitionOffsetRequest` for this
            partition

        :param new_offset: The offset to which to set this partition. This
            setting indicates how to reset the consumer's internal offset
            counter when an OffsetOutOfRangeError is encountered.
            There are two special values. Specify -1 to receive the latest
            offset (i.e. the offset of the next coming message) and -2 to
            receive the earliest available offset.
        :type new_offset: :class:`pykafka.common.OffsetType` or int
        """
        return PartitionOffsetRequest(
            self.partition.topic.name, self.partition.id,
            new_offset, 1)

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
            int(time.time() * 1000),
            b'pykafka'
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
                log.debug("Skipping enqueue for offset (%s) "
                          "less than last_offset_consumed (%s)",
                          message.offset, self.last_offset_consumed)
                continue
            message.partition = self.partition
            if message.partition_id != self.partition.id:
                log.error("Partition %s enqueued a message meant for partition %s",
                          self.partition.id, message.partition_id)
            message.partition_id = self.partition.id
            self._messages.put(message)
            self.next_offset = message.offset + 1

            if self._messages_arrived is not None:
                self._messages_arrived.release()
