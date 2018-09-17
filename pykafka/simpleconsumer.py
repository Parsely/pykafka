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
import datetime as dt
import itertools
import logging
import json
import socket
import sys
import threading
import time
from collections import defaultdict
import weakref

from six import reraise

from .common import OffsetType, EPOCH
from .utils.compat import (Queue, Empty, iteritems, itervalues,
                           range, iterkeys, get_bytes, get_string)
from .exceptions import (UnknownError, OffsetOutOfRangeError, UnknownTopicOrPartition,
                         OffsetMetadataTooLarge, GroupLoadInProgress,
                         NotCoordinatorForGroup, SocketDisconnectedError,
                         ConsumerStoppedException, KafkaException,
                         NotLeaderForPartition, OffsetRequestFailedError,
                         RequestTimedOut, UnknownMemberId, RebalanceInProgress,
                         IllegalGeneration, ERROR_CODES, UnicodeException)
from .protocol import (PartitionFetchRequest, PartitionOffsetCommitRequest,
                       PartitionOffsetFetchRequest, PartitionOffsetRequest)
from .utils.error_handlers import (handle_partition_responses, raise_error,
                                   build_parts_by_error, valid_int)


log = logging.getLogger(__name__)
MAGIC_OFFSETS = [OffsetType.EARLIEST, OffsetType.LATEST]


class SimpleConsumer(object):
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
                 fetch_error_backoff_ms=500,
                 fetch_wait_max_ms=100,
                 offsets_channel_backoff_ms=1000,
                 offsets_commit_max_retries=5,
                 auto_offset_reset=OffsetType.EARLIEST,
                 consumer_timeout_ms=-1,
                 auto_start=True,
                 reset_offset_on_start=False,
                 compacted_topic=False,
                 generation_id=-1,
                 consumer_id=b'',
                 deserializer=None,
                 reset_offset_on_fetch=True):
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
        :type consumer_group: str
        :param partitions: Existing partitions to which to connect
        :type partitions: Iterable of :class:`pykafka.partition.Partition`
        :param fetch_message_max_bytes: The number of bytes of messages to
            attempt to fetch
        :type fetch_message_max_bytes: int
        :param num_consumer_fetchers: The number of workers used to make
            FetchRequests
        :type num_consumer_fetchers: int
        :param auto_commit_enable: If true, periodically commit to kafka the
            offset of messages already returned from consume() calls. Requires that
            `consumer_group` is not `None`.
        :type auto_commit_enable: bool
        :param auto_commit_interval_ms: The frequency (in milliseconds) at which the
            consumer offsets are committed to kafka. This setting is ignored if
            `auto_commit_enable` is `False`.
        :type auto_commit_interval_ms: int
        :param queued_max_messages: Maximum number of messages buffered for
            consumption per partition
        :type queued_max_messages: int
        :param fetch_min_bytes: The minimum amount of data (in bytes) the server
            should return for a fetch request. If insufficient data is available
            the request will block until sufficient data is available.
        :type fetch_min_bytes: int
        :param fetch_error_backoff_ms: The amount of time (in milliseconds) that
            the consumer should wait before retrying after an error. Errors include
            absence of data (`RD_KAFKA_RESP_ERR__PARTITION_EOF`), so this can slow
            a normal fetch scenario. Only used by the native consumer
            (`RdKafkaSimpleConsumer`).
        :type fetch_error_backoff_ms: int
        :type fetch_error_backoff_ms: int
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
        :param compacted_topic: Set to read from a compacted topic. Forces
            consumer to use less stringent message ordering logic because compacted
            topics do not provide offsets in strict incrementing order.
        :type compacted_topic: bool
        :param generation_id: Deprecated::2.7 Do not set if directly instantiating
            SimpleConsumer. The generation id with which to make group requests
        :type generation_id: int
        :param consumer_id: Deprecated::2.7 Do not set if directly instantiating
            SimpleConsumer. The identifying string to use for this consumer on
            group requests
        :type consumer_id: bytes
        :param deserializer: A function defining how to deserialize messages returned
            from Kafka. A function with the signature d(value, partition_key) that
            returns a tuple of (deserialized_value, deserialized_partition_key). The
            arguments passed to this function are the bytes representations of a
            message's value and partition key, and the returned data should be these
            fields transformed according to the client code's serialization logic.
            See `pykafka.utils.__init__` for stock implemtations.
        :type deserializer: function
        :param reset_offset_on_fetch: Whether to update offsets during fetch_offsets.
               Disable for read-only use cases to prevent side-effects.
        :type reset_offset_on_fetch: bool
        """
        self._running = False
        self._cluster = cluster
        self._consumer_group = None
        if consumer_group:
            try:
                self._consumer_group = get_string(consumer_group).encode('ascii')
            except UnicodeEncodeError:
                raise UnicodeException("Consumer group name '{}' contains non-ascii "
                                       "characters".format(consumer_group))
        self._topic = topic
        self._fetch_message_max_bytes = valid_int(fetch_message_max_bytes)
        self._fetch_min_bytes = valid_int(fetch_min_bytes)
        self._queued_max_messages = valid_int(queued_max_messages)
        self._num_consumer_fetchers = valid_int(num_consumer_fetchers)
        self._fetch_wait_max_ms = valid_int(fetch_wait_max_ms, allow_zero=True)
        self._consumer_timeout_ms = valid_int(consumer_timeout_ms,
                                              allow_zero=True, allow_negative=True)
        self._offsets_channel_backoff_ms = valid_int(offsets_channel_backoff_ms)
        self._auto_offset_reset = auto_offset_reset
        offsets_commit_max_retries = valid_int(offsets_commit_max_retries,
                                               allow_zero=True)
        self._offsets_commit_max_retries = offsets_commit_max_retries
        # not directly configurable
        self._offsets_fetch_max_retries = offsets_commit_max_retries
        self._offsets_reset_max_retries = offsets_commit_max_retries
        self._auto_start = auto_start
        self._reset_offset_on_start = reset_offset_on_start
        self._is_compacted_topic = compacted_topic
        self._generation_id = -1
        self._consumer_id = b''
        self._deserializer = deserializer
        self._reset_offset_on_fetch = reset_offset_on_fetch

        # incremented for any message arrival from any partition
        # the initial value is 0 (no messages waiting)
        self._messages_arrived = self._cluster.handler.Semaphore(value=0)
        self._slot_available = self._cluster.handler.Event()

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = valid_int(auto_commit_interval_ms)
        self._last_auto_commit = time.time()
        self._worker_exception = None
        self._update_lock = self._cluster.handler.Lock()

        self._discover_group_coordinator()

        if partitions is not None:
            self._partitions = {p: OwnedPartition(p,
                                                  self._cluster.handler,
                                                  self._messages_arrived,
                                                  self._is_compacted_topic)
                                for p in partitions}
        else:
            self._partitions = {topic.partitions[k]:
                                OwnedPartition(p,
                                               self._cluster.handler,
                                               self._messages_arrived,
                                               self._is_compacted_topic)
                                for k, p in iteritems(topic.partitions)}
        self._partitions_by_id = {p.partition.id: p
                                  for p in itervalues(self._partitions)}
        # Organize partitions by leader for efficient queries
        self._setup_partitions_by_leader()
        self.partition_cycle = itertools.cycle(self._partitions.values())

        self._default_error_handlers = self._build_default_error_handlers()

        if self._auto_start:
            self.start()

    @property
    def consumer_id(self):
        return self._consumer_id

    @consumer_id.setter
    def consumer_id(self, value):
        self._consumer_id = value
        for op in itervalues(self._partitions):
            op.set_consumer_id(self._consumer_id)

    @property
    def generation_id(self):
        return self._generation_id

    @generation_id.setter
    def generation_id(self, value):
        self._generation_id = valid_int(value, allow_zero=True,
                                        allow_negative=True)

    def __repr__(self):
        return "<{module}.{name} at {id_} (consumer_group={group})>".format(
            module=self.__class__.__module__,
            name=self.__class__.__name__,
            id_=hex(id(self)),
            group=self._consumer_group
        )

    def _raise_worker_exceptions(self):
        """Raises exceptions encountered on worker threads"""
        if self._worker_exception is not None:
            reraise(*self._worker_exception)

    def _update(self):
        """Update the consumer and cluster after an ERROR_CODE"""
        # only allow one thread to be updating the consumer at a time
        with self._update_lock:
            self._cluster.update()
            self._setup_partitions_by_leader()
            self._discover_group_coordinator()

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

        self._raise_worker_exceptions()

    def _setup_partitions_by_leader(self):
        self._partitions_by_leader = defaultdict(list)
        for p in itervalues(self._partitions):
            self._partitions_by_leader[p.partition.leader].append(p)

    def _build_default_error_handlers(self):
        """Set up the error handlers to use for partition errors."""
        self = weakref.proxy(self)

        def _handle_OffsetOutOfRangeError(parts):
            log.info("Resetting offsets in response to OffsetOutOfRangeError")
            self.reset_offsets(
                partition_offsets=[(owned_partition.partition, self._auto_offset_reset)
                                   for owned_partition, pres in parts]
            )

        def _handle_RequestTimedOut(parts):
            log.info("Continuing in response to RequestTimedOut")

        def _handle_NotCoordinatorForGroup(parts):
            log.info("Updating cluster in response to NotCoordinatorForGroup")
            self._update()

        def _handle_NotLeaderForPartition(parts):
            log.info("Updating cluster in response to NotLeaderForPartition")
            self._update()

        def _handle_GroupLoadInProgress(parts):
            log.info("Continuing in response to GroupLoadInProgress")

        def _handle_IllegalGeneration(parts):
            log.info("Continuing in response to IllegalGeneration")

        def _handle_UnknownMemberId(parts):
            log.info("Continuing in response to UnknownMemberId")

        def _handle_UnknownError(parts):
            log.info("Continuing in response to UnknownError")

        def _handle_RebalanceInProgress(parts):
            log.info("Continuing in response to RebalanceInProgress")

        return {
            UnknownTopicOrPartition.ERROR_CODE: lambda p: raise_error(UnknownTopicOrPartition),
            UnknownError.ERROR_CODE: _handle_UnknownError,
            OffsetOutOfRangeError.ERROR_CODE: _handle_OffsetOutOfRangeError,
            NotLeaderForPartition.ERROR_CODE: _handle_NotLeaderForPartition,
            OffsetMetadataTooLarge.ERROR_CODE: lambda p: raise_error(OffsetMetadataTooLarge),
            NotCoordinatorForGroup.ERROR_CODE: _handle_NotCoordinatorForGroup,
            RequestTimedOut.ERROR_CODE: _handle_RequestTimedOut,
            GroupLoadInProgress.ERROR_CODE: _handle_GroupLoadInProgress,
            UnknownMemberId.ERROR_CODE: _handle_UnknownMemberId,
            RebalanceInProgress.ERROR_CODE: _handle_RebalanceInProgress,
            IllegalGeneration.ERROR_CODE: _handle_IllegalGeneration
        }

    def _discover_group_coordinator(self):
        """Set the group coordinator for this consumer.

        If a consumer group is not supplied to __init__, this method does nothing
        """
        if self._consumer_group is not None:
            self._group_coordinator = self._cluster.get_group_coordinator(self._consumer_group)

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
        return {p.partition.id:
                (OffsetType.EARLIEST if p.last_offset_consumed == -1
                 else p.last_offset_consumed)
                for p in itervalues(self._partitions_by_id)}

    def __del__(self):
        """Stop consumption and workers when object is deleted"""
        log.debug("Finalising {}".format(self))
        if self._running:
            self.stop()

    def cleanup(self):
        if not self._slot_available.is_set():
            self._slot_available.set()

    def stop(self):
        """Flag all running workers for deletion."""
        self._running = False
        if self._auto_commit_enable and self._consumer_group is not None:
            self.commit_offsets()
        # unblock a waiting consume() call
        if self._messages_arrived is not None:
            self._messages_arrived.release()

    def _setup_autocommit_worker(self):
        """Start the autocommitter thread"""
        self = weakref.proxy(self)

        def autocommitter():
            while True:
                try:
                    if not self._running:
                        break
                    if self._auto_commit_enable:
                        self._auto_commit()
                    self._cluster.handler.sleep(self._auto_commit_interval_ms / 1000)
                except ReferenceError:
                    break
                except Exception:
                    # surface all exceptions to the main thread
                    self._worker_exception = sys.exc_info()
                    break
            log.debug("Autocommitter thread exiting")
        log.debug("Starting autocommitter thread")
        return self._cluster.handler.spawn(autocommitter, name="pykafka.SimpleConsumer.autocommiter")

    def _setup_fetch_workers(self):
        """Start the fetcher threads"""
        # NB this gets overridden in rdkafka.RdKafkaSimpleConsumer
        self = weakref.proxy(self)

        def fetcher():
            while True:
                try:
                    if not self._running:
                        break
                    self.fetch()
                    self._cluster.handler.sleep(.01)
                except ReferenceError:
                    break
                except Exception:
                    # surface all exceptions to the main thread
                    self._worker_exception = sys.exc_info()
                    break
            try:
                self.cleanup()
            except ReferenceError as e:
                log.debug("Attempt to cleanup consumer failed")
                log.exception(e)
            log.debug("Fetcher thread exiting")
        log.info("Starting %s fetcher threads", self._num_consumer_fetchers)
        return [self._cluster.handler.spawn(fetcher, name="pykafka.SimpleConsumer.fetcher")
                for i in range(self._num_consumer_fetchers)]

    def __iter__(self):
        """Yield an infinite stream of messages until the consumer times out"""
        while True:
            message = self.consume(block=True)
            if not message:
                return
            yield message

    def consume(self, block=True, unblock_event=None):
        """Get one message from the consumer.

        :param block: Whether to block while waiting for a message
        :type block: bool
        :param unblock_event: Return when the event is set()
        :type unblock_event: :class:`threading.Event`
        """
        timeout = None
        if block:
            if self._consumer_timeout_ms > 0:
                timeout = float(self._consumer_timeout_ms) / 1000
            else:
                timeout = 1.0

        ret = None
        while True:
            self._raise_worker_exceptions()
            self._cluster.handler.sleep()
            if self._messages_arrived.acquire(blocking=block, timeout=timeout):
                # by passing through this semaphore, we know that at
                # least one message is waiting in some queue.
                if not self._running:
                    raise ConsumerStoppedException()
                message = None
                while not message:
                    owned_partition = next(self.partition_cycle)
                    message = owned_partition.consume()
                ret = message
                break
            else:
                if not self._running:
                    raise ConsumerStoppedException()
                elif not block or self._consumer_timeout_ms > 0:
                    ret = None
                    break
            if unblock_event and unblock_event.is_set():
                return

        if any(op.message_count <= self._queued_max_messages
               for op in itervalues(self._partitions)):
            if not self._slot_available.is_set():
                self._slot_available.set()

        if self._deserializer is not None:
            ret.value, ret.partition_key = self._deserializer(ret.value,
                                                              ret.partition_key)
        return ret

    def _auto_commit(self):
        """Commit offsets only if it's time to do so"""
        if not self._auto_commit_enable or self._auto_commit_interval_ms == 0:
            return

        if (time.time() - self._last_auto_commit) * 1000.0 >= self._auto_commit_interval_ms:
            log.debug("Autocommitting consumer offset for consumer group %s and topic %s",
                      self._consumer_group, self._topic.name)
            if self._consumer_group is not None:
                self.commit_offsets()
            self._last_auto_commit = time.time()

    def commit_offsets(self, partition_offsets=None):
        """Commit offsets for this consumer's partitions

        Uses the offset commit/fetch API

        :param partition_offsets: (`partition`, `offset`) pairs to
            commit where `partition` is the partition for which to commit the offset
            and `offset` is the offset to commit for the partition. Note that using
            this argument when `auto_commit_enable` is enabled can cause inconsistencies
            in committed offsets. For best results, use *either* this argument *or*
            `auto_commit_enable`.
        :type partition_offsets: Sequence of tuples of the form
            (:class:`pykafka.partition.Partition`, int)
        """
        if not self._consumer_group:
            raise Exception("consumer group must be specified to commit offsets")

        if partition_offsets is None:
            partition_offsets = [(p, None) for p in self._partitions.keys()]

        # turn Partitions into their corresponding OwnedPartitions
        try:
            owned_partition_offsets = {self._partitions[p]: offset
                                       for p, offset in partition_offsets}
        except KeyError as e:
            raise KafkaException("Unknown partition supplied to commit_offsets\n%s", e)
        reqs = [p.build_offset_commit_request(offset=o) for p, o
                in iteritems(owned_partition_offsets)]

        log.debug("Committing offsets for %d partitions to broker id %s", len(reqs),
                  self._group_coordinator.id)
        for i in range(self._offsets_commit_max_retries):
            if i > 0:
                log.debug("Retrying")
            self._cluster.handler.sleep(i * (self._offsets_channel_backoff_ms / 1000))

            try:
                response = self._group_coordinator.commit_consumer_group_offsets(
                    self._consumer_group, self._generation_id, self._consumer_id, reqs)
            except (SocketDisconnectedError, IOError):
                log.error("Error committing offsets for topic '%s' from consumer id '%s'"
                          "(SocketDisconnectedError)",
                          self._topic.name, self._consumer_id)
                if i >= self._offsets_commit_max_retries - 1:
                    raise
                self._update()
                continue

            parts_by_error = handle_partition_responses(
                self._default_error_handlers,
                response=response,
                partitions_by_id=self._partitions_by_id)
            if (len(parts_by_error) == 1 and 0 in parts_by_error) or \
                    len(parts_by_error) == 0:
                break
            log.error("Error committing offsets for topic '%s' from consumer id '%s'"
                      "(errors: %s)", self._topic.name, self._consumer_id,
                      {ERROR_CODES[err]: [op.partition.id for op, _ in parts]
                       for err, parts in iteritems(parts_by_error)})

            # retry only the partitions that errored
            if 0 in parts_by_error:
                parts_by_error.pop(0)
            errored_partitions = [
                op for code, err_group in iteritems(parts_by_error)
                for op, res in err_group
            ]
            reqs = [op.build_offset_commit_request(offset=owned_partition_offsets[op])
                    for op in errored_partitions]

    def fetch_offsets(self):
        """Fetch offsets for this consumer's topic

        Uses the offset commit/fetch API

        :return: List of (id, :class:`pykafka.protocol.OffsetFetchPartitionResponse`)
            tuples
        """
        if not self._consumer_group:
            raise Exception("consumer group must be specified to fetch offsets")
        if not self._partitions:
            return []

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
                              pres.offset - 1)
                    # offset fetch requests return the next offset to consume,
                    # so account for this here by passing offset - 1
                    owned_partition.set_offset(pres.offset - 1)

            # If any partitions didn't have a committed offset,
            # then reset those partition's offsets.
            if partition_offsets_to_reset:
                self.reset_offsets(partition_offsets_to_reset)

        reqs = [p.build_offset_fetch_request() for p in self._partitions.values()]
        success_responses = []

        log.debug("Fetching offsets for %d partitions from broker id %s", len(reqs),
                  self._group_coordinator.id)

        for i in range(self._offsets_fetch_max_retries):
            if i > 0:
                log.debug("Retrying offset fetch")

            res = self._group_coordinator.fetch_consumer_group_offsets(self._consumer_group, reqs)
            parts_by_error = handle_partition_responses(
                self._default_error_handlers,
                response=res,
                success_handler=_handle_success if self._reset_offset_on_fetch else None,
                partitions_by_id=self._partitions_by_id)

            success_responses.extend([(op.partition.id, r)
                                      for op, r in parts_by_error.get(0, [])])
            if len(reqs) == 0 or (len(parts_by_error) == 1 and 0 in parts_by_error):
                return success_responses
            log.error("Error fetching offsets for topic '%s' (errors: %s)",
                      self._topic.name,
                      {ERROR_CODES[err]: [op.partition.id for op, _ in parts]
                       for err, parts in iteritems(parts_by_error)})

            self._cluster.handler.sleep(i * (self._offsets_channel_backoff_ms / 1000))

            to_retry = [pair for err in itervalues(parts_by_error) for pair in err]
            reqs = [p.build_offset_fetch_request() for p, _ in to_retry]

        raise KafkaException(parts_by_error)

    def reset_offsets(self, partition_offsets=None):
        """Reset offsets for the specified partitions

        For each value provided in `partition_offsets`: if the value is an integer,
        immediately reset the partition's internal offset counter to that value. If
        it's a `datetime.datetime` instance or a valid `OffsetType`, issue a
        `ListOffsetRequest` using that timestamp value to discover the latest offset
        in the latest log segment before that timestamp, then set the partition's
        internal counter to that value.

        :param partition_offsets: (`partition`, `timestamp_or_offset`) pairs to
            reset where `partition` is the partition for which to reset the offset
            and `timestamp_or_offset` is EITHER the timestamp before which to find
            a valid offset to set the partition's counter to OR the new offset the
            partition's counter should be set to.
        :type partition_offsets: Sequence of tuples of the form
            (:class:`pykafka.partition.Partition`, int OR `datetime.datetime`)
        """
        def _handle_success(parts):
            for owned_partition, pres in parts:
                if len(pres.offset) > 0:
                    # there is at least one log segment that starts before the given
                    # timestamp. set the counter to the latest offset of the latest
                    # log segment before the given timestamp.
                    owned_partition_offsets[owned_partition] = pres.offset[0] - 1
                else:
                    log.warning("Partition {id_}: no offsets available before {offset}."
                                "Defaulting to OffsetType.EARLIEST.".format(
                                    id_=owned_partition.partition.id,
                                    offset=owned_partition_timestamps[owned_partition]))
                    owned_partition_offsets[owned_partition] = OffsetType.EARLIEST

        if partition_offsets is None:
            partition_offsets = [(a, self._auto_offset_reset)
                                 for a in self._partitions.keys()]

        try:
            owned_partition_offsets = {self._partitions[p]: offset
                                       for p, offset in partition_offsets}
        except KeyError as e:
            raise KafkaException("Unknown partition supplied to reset_offsets\n%s", e)

        log.info("Resetting offsets for %s partitions", len(list(owned_partition_offsets)))

        owned_partition_timestamps = {
            op: timestamp for op, timestamp
            in iteritems(owned_partition_offsets)
            if isinstance(timestamp, dt.datetime) or timestamp in MAGIC_OFFSETS}
        if owned_partition_timestamps:
            for i in range(self._offsets_reset_max_retries):
                by_leader = defaultdict(list)
                for partition, timestamp in iteritems(owned_partition_timestamps):
                    by_leader[partition.partition.leader].append((partition, timestamp))
                for broker, timestamps in iteritems(by_leader):
                    reqs = [owned_partition.build_offset_request(timestamp)
                            for owned_partition, timestamp in timestamps]
                    response = broker.request_offset_limits(reqs)
                    parts_by_error = handle_partition_responses(
                        self._default_error_handlers,
                        response=response,
                        success_handler=_handle_success,
                        partitions_by_id=self._partitions_by_id)
                    if 0 in parts_by_error:
                        successful = [part for part, _ in parts_by_error.pop(0)]
                        list(map(owned_partition_timestamps.pop, successful))
                    if not parts_by_error:
                        continue
                    log.error("Error in ListOffsetRequest for topic '%s' (errors: %s)",
                              self._topic.name,
                              {ERROR_CODES[err]: [op.partition.id for op, _ in parts]
                               for err, parts in iteritems(parts_by_error)})
                    self._cluster.handler.sleep(i * (self._offsets_channel_backoff_ms / 1000))
                if not owned_partition_timestamps:
                    break
                log.debug("Retrying offset request")
            if owned_partition_timestamps:
                raise OffsetRequestFailedError("Offset request failed after %d "
                                               "retries", self._offsets_reset_max_retries)

        sorted_offsets = sorted(iteritems(owned_partition_offsets),
                                key=lambda k: k[0].partition.id)
        for owned_partition, offset in sorted_offsets:
            if not isinstance(offset, int):
                raise ValueError("Invalid offset value encountered in reset_offsets:\n\t"
                                 "Partition {pid} got offset '{offset}'."
                                 .format(pid=owned_partition.partition.id, offset=offset))
            with owned_partition.fetch_lock:
                owned_partition.flush()
                owned_partition.set_offset(offset)

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

        def unlock_partitions(parts):
            for owned_partition in parts:
                owned_partition.fetch_lock.release()

        self._wait_for_slot_available()
        sorted_by_leader = sorted(iteritems(self._partitions_by_leader),
                                  key=lambda k: k[0].id)
        for broker, owned_partitions in sorted_by_leader:
            partition_reqs = {}
            sorted_offsets = sorted(owned_partitions, key=lambda k: k.partition.id)
            for owned_partition in sorted_offsets:
                # attempt to acquire lock, just pass if we can't
                if owned_partition.fetch_lock.acquire(False):
                    partition_reqs[owned_partition] = None
                    if owned_partition.message_count < self._queued_max_messages:
                        fetch_req = owned_partition.build_fetch_request(
                            self._fetch_message_max_bytes)
                        partition_reqs[owned_partition] = fetch_req
            if partition_reqs:
                try:
                    response = broker.fetch_messages(
                        [a for a in itervalues(partition_reqs) if a],
                        timeout=self._fetch_wait_max_ms,
                        min_bytes=self._fetch_min_bytes
                    )
                except (IOError, SocketDisconnectedError):
                    unlock_partitions(iterkeys(partition_reqs))
                    if self._running:
                        log.info("Updating cluster in response to error in fetch() "
                                 "for broker id %s", broker.id)
                        self._update()
                    # If the broker dies while we're supposed to stop,
                    # it's fine, and probably an integration test.
                    return
                except socket.error:
                    raise ValueError("Failed to decode IO buffer. Ensure that "
                                     "the KafkaClient's broker_version kwarg "
                                     "matches the version of the Kafka cluster.")
                parts_by_error = build_parts_by_error(response, self._partitions_by_id)
                handle_partition_responses(
                    self._default_error_handlers,
                    parts_by_error=parts_by_error,
                    success_handler=_handle_success)
                unlock_partitions(iterkeys(partition_reqs))

    def _wait_for_slot_available(self):
        """Block until at least one queue has less than `_queued_max_messages`"""
        if all(op.message_count >= self._queued_max_messages
               for op in itervalues(self._partitions)):
            for op in itervalues(self._partitions):
                op.fetch_lock.acquire()
            if all(op.message_count >= self._queued_max_messages
                   for op in itervalues(self._partitions)):
                self._slot_available.clear()
            for op in itervalues(self._partitions):
                op.fetch_lock.release()
            while not self._slot_available.is_set():
                self._cluster.handler.sleep()
                self._slot_available.wait(5)


class OwnedPartition(object):
    """A partition that is owned by a SimpleConsumer.

    Used to keep track of offsets and the internal message queue.
    """

    def __init__(self,
                 partition,
                 handler=None,
                 semaphore=None,
                 compacted_topic=False,
                 consumer_id=b''):
        """
        :param partition: The partition to hold
        :type partition: :class:`pykafka.partition.Partition`
        :param consumer_id: The ID of the parent consumer
        :type consumer_id: bytes
        :param handler: The :class:`pykafka.handlers.Handler` instance to use
            to generate a lock
        type handler: :class:`pykafka.handler.Handler`
        :param semaphore: A Semaphore that counts available messages and
            facilitates non-busy blocking
        :type semaphore: :class:`pykafka.utils.compat.Semaphore`
        :param compacted_topic: Set to read from a compacted topic. Forces
            consumer to use less stringent ordering logic when because compacted
            topics do not provide offsets in strict incrementing order.
        :type compacted_topic: bool
        """
        self.partition = partition
        self._consumer_id = consumer_id
        self._messages = Queue()
        self._messages_arrived = semaphore
        self._is_compacted_topic = compacted_topic
        self.last_offset_consumed = -1
        self.next_offset = 0
        self.fetch_lock = handler.RLock() if handler is not None else threading.RLock()
        self.set_consumer_id(self._consumer_id)

    def set_consumer_id(self, value):
        self._consumer_id = value
        # include consumer id in offset metadata for debugging
        self._offset_metadata = {
            'consumer_id': get_string(self._consumer_id),
            'hostname': socket.gethostname()
        }
        # precalculate json to avoid expensive operation in loops
        self._offset_metadata_json = json.dumps(self._offset_metadata)

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

    def build_offset_request(self, offsets_before):
        """Create a :class:`pykafka.protocol.PartitionOffsetRequest` for this
            partition

        :param offsets_before: Timestamp indicating the
            latest write time for returned offsets. Only offsets of messages
            written before this timestamp will be returned. Permissible
            special values are `common.OffsetType.LATEST`, indicating that
            offsets from all available log segments should be returned, and
            `common.OffsetType.EARLIEST`, indicating that only the offset of
            the earliest available message should be returned.
        :type offsets_before: `datetime.datetime`
        """
        if isinstance(offsets_before, dt.datetime):
            offsets_before = round((offsets_before - EPOCH).total_seconds() * 1000)
        elif offsets_before not in MAGIC_OFFSETS:
            raise ValueError("offsets_before is an invalid timestamp: {}"
                             .format(offsets_before))
        return PartitionOffsetRequest(self.partition.topic.name, self.partition.id,
                                      offsets_before, 1)

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

    def build_offset_commit_request(self, offset=None):
        """Create a :class:`pykafka.protocol.PartitionOffsetCommitRequest`
            for this partition

        :param offset: The offset to send in the request. If None, defaults to
            last_offset_consumed + 1
        :type offset: int
        """
        return PartitionOffsetCommitRequest(
            self.partition.topic.name,
            self.partition.id,
            offset if offset is not None else self.last_offset_consumed + 1,
            int(time.time() * 1000),
            get_bytes('{}'.format(self._offset_metadata_json))
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
            # enforce ordering of messages
            if (self._is_compacted_topic and message.offset < self.next_offset) or \
                    (not self._is_compacted_topic and message.offset != self.next_offset):
                log.debug("Skipping enqueue for offset (%s) "
                          "not equal to next_offset (%s)",
                          message.offset, self.next_offset)
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
