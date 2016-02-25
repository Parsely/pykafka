"""
Author: Emmett Butler
"""
__license__ = """
Copyright 2016 Parse.ly, Inc.

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
__all__ = ["ManagedBalancedConsumer"]
import logging
import sys
import weakref

from .balancedconsumer import BalancedConsumer
from .common import OffsetType
from .exceptions import (IllegalGeneration, RebalanceInProgress, UnknownMemberId,
                         NotCoordinatorForGroup, GroupCoordinatorNotAvailable,
                         GroupAuthorizationFailed)
from .protocol import MemberAssignment
from .utils.compat import itervalues, iterkeys

log = logging.getLogger(__name__)


class ManagedBalancedConsumer(BalancedConsumer):
    """A self-balancing consumer that uses Kafka 0.9's Group Membership API

    Implements the Group Management API semantics for Kafka 0.9 compatibility

    Maintains a single instance of SimpleConsumer, periodically using the
    consumer rebalancing algorithm to reassign partitions to this
    SimpleConsumer.

    This class overrides the functionality of
    :class:`pykafka.balancedconsumer.BalancedConsumer` that deals with ZooKeeper and
    inherits other functionality directly.
    """
    def __init__(self,
                 topic,
                 cluster,
                 consumer_group,
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
                 rebalance_max_retries=5,
                 rebalance_backoff_ms=2 * 1000,
                 auto_start=True,
                 reset_offset_on_start=False,
                 post_rebalance_callback=None,
                 use_rdkafka=False,
                 compacted_topic=True,
                 heartbeat_interval_ms=3000):
        """Create a ManagedBalancedConsumer instance

        :param topic: The topic this consumer should consume
        :type topic: :class:`pykafka.topic.Topic`
        :param cluster: The cluster to which this consumer should connect
        :type cluster: :class:`pykafka.cluster.Cluster`
        :param consumer_group: The name of the consumer group this consumer
            should join.
        :type consumer_group: bytes
        :param fetch_message_max_bytes: The number of bytes of messages to
            attempt to fetch with each fetch request
        :type fetch_message_max_bytes: int
        :param num_consumer_fetchers: The number of workers used to make
            FetchRequests
        :type num_consumer_fetchers: int
        :param auto_commit_enable: If true, periodically commit to kafka the
            offset of messages already fetched by this consumer. This also
            requires that `consumer_group` is not `None`.
        :type auto_commit_enable: bool
        :param auto_commit_interval_ms: The frequency (in milliseconds) at which
            the consumer's offsets are committed to kafka. This setting is
            ignored if `auto_commit_enable` is `False`.
        :type auto_commit_interval_ms: int
        :param queued_max_messages: The maximum number of messages buffered for
            consumption in the internal
            :class:`pykafka.simpleconsumer.SimpleConsumer`
        :type queued_max_messages: int
        :param fetch_min_bytes: The minimum amount of data (in bytes) that the
            server should return for a fetch request. If insufficient data is
            available, the request will block until sufficient data is available.
        :type fetch_min_bytes: int
        :param fetch_wait_max_ms: The maximum amount of time (in milliseconds)
            that the server will block before answering a fetch request if
            there isn't sufficient data to immediately satisfy `fetch_min_bytes`.
        :type fetch_wait_max_ms: int
        :param offsets_channel_backoff_ms: Backoff time to retry failed offset
            commits and fetches.
        :type offsets_channel_backoff_ms: int
        :param offsets_commit_max_retries: The number of times the offset commit
            worker should retry before raising an error.
        :type offsets_commit_max_retries: int
        :param auto_offset_reset: What to do if an offset is out of range. This
            setting indicates how to reset the consumer's internal offset
            counter when an `OffsetOutOfRangeError` is encountered.
        :type auto_offset_reset: :class:`pykafka.common.OffsetType`
        :param consumer_timeout_ms: Amount of time (in milliseconds) the
            consumer may spend without messages available for consumption
            before returning None.
        :type consumer_timeout_ms: int
        :param rebalance_max_retries: The number of times the rebalance should
            retry before raising an error.
        :type rebalance_max_retries: int
        :param rebalance_backoff_ms: Backoff time (in milliseconds) between
            retries during rebalance.
        :type rebalance_backoff_ms: int
        :param auto_start: Whether the consumer should start after __init__ is complete.
            If false, it can be started with `start()`.
        :type auto_start: bool
        :param reset_offset_on_start: Whether the consumer should reset its
            internal offset counter to `self._auto_offset_reset` and commit that
            offset immediately upon starting up
        :type reset_offset_on_start: bool
        :param post_rebalance_callback: A function to be called when a rebalance is
            in progress. This function should accept three arguments: the
            :class:`pykafka.balancedconsumer.BalancedConsumer` instance that just
            completed its rebalance, a dict of partitions that it owned before the
            rebalance, and a dict of partitions it owns after the rebalance. These dicts
            map partition ids to the most recently known offsets for those partitions.
            This function can optionally return a dictionary mapping partition ids to
            offsets. If it does, the consumer will reset its offsets to the supplied
            values before continuing consumption.
            Note that the BalancedConsumer is in a poorly defined state at
            the time this callback runs, so that accessing its properties
            (such as `held_offsets` or `partitions`) might yield confusing
            results.  Instead, the callback should really rely on the
            provided partition-id dicts, which are well-defined.
        :type post_rebalance_callback: function
        :param use_rdkafka: Use librdkafka-backed consumer if available
        :type use_rdkafka: bool
        :param compacted_topic: Set to read from a compacted topic. Forces
            consumer to use less stringent message ordering logic because compacted
            topics do not provide offsets in stict incrementing order.
        :type compacted_topic: bool
        :param heartbeat_interval_ms: The amount of time in milliseconds to wait between
            heartbeat requests
        :type heartbeat_interval_ms: int
        """

        self._cluster = cluster
        if not isinstance(consumer_group, bytes):
            raise TypeError("consumer_group must be a bytes object")
        self._consumer_group = consumer_group
        self._topic = topic

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._fetch_message_max_bytes = fetch_message_max_bytes
        self._fetch_min_bytes = fetch_min_bytes
        self._num_consumer_fetchers = num_consumer_fetchers
        self._queued_max_messages = queued_max_messages
        self._fetch_wait_max_ms = fetch_wait_max_ms
        self._consumer_timeout_ms = consumer_timeout_ms
        self._offsets_channel_backoff_ms = offsets_channel_backoff_ms
        self._offsets_commit_max_retries = offsets_commit_max_retries
        self._auto_offset_reset = auto_offset_reset
        self._reset_offset_on_start = reset_offset_on_start
        self._rebalance_max_retries = rebalance_max_retries
        self._rebalance_backoff_ms = rebalance_backoff_ms
        self._post_rebalance_callback = post_rebalance_callback
        self._is_compacted_topic = compacted_topic
        self._heartbeat_interval_ms = heartbeat_interval_ms
        if use_rdkafka is True:
            raise ImportError("use_rdkafka is not available for {}".format(
                self.__class__.__name__))
        self._use_rdkafka = use_rdkafka

        self._rebalancing_lock = cluster.handler.Lock()
        self._consumer = None
        self._consumer_id = b''
        self._worker_trace_logged = False
        self._worker_exception = None

        if auto_start is True:
            self.start()

    def _setup_heartbeat_worker(self):
        """Start the heartbeat worker"""
        self = weakref.proxy(self)

        def fetcher():
            while True:
                try:
                    if not self._running:
                        break
                    self._send_heartbeat()
                    self._cluster.handler.sleep(self._heartbeat_interval_ms / 1000)
                except ReferenceError:
                    break
                except Exception:
                    # surface all exceptions to the main thread
                    self._worker_exception = sys.exc_info()
                    break
            log.debug("Heartbeat worker exiting")
        log.info("Starting heartbeat worker")
        return self._cluster.handler.spawn(
            fetcher, name="pykafka.ManagedBalancedConsumer.heartbeats")

    def start(self):
        """Start this consumer. Must be called before consume() if `auto_start=False`."""
        try:
            self._running = True
            self._group_coordinator = self._cluster.get_group_coordinator(
                self._consumer_group)
            self._rebalance()
            self._setup_heartbeat_worker()
        except Exception:
            log.exception("Stopping consumer in response to error")
            self.stop()

    def stop(self):
        """Stop this consumer

        Should be called as part of a graceful shutdown
        """
        self._running = False
        if self._consumer is not None:
            self._consumer.stop()
        self._group_coordinator.leave_managed_consumer_group(self._consumer_group,
                                                             self._consumer_id)

    def _send_heartbeat(self):
        """Send a heartbeat request to the group coordinator and react to the response"""
        res = self._group_coordinator.group_heartbeat(
            self._consumer_group, self._generation_id, self._consumer_id)
        if res.error_code == 0:
            return
        log.info("Error code %d encountered on heartbeat." % res.error_code)
        if res.error_code in (IllegalGeneration.ERROR_CODE,
                              RebalanceInProgress.ERROR_CODE,
                              UnknownMemberId.ERROR_CODE):
            pass
        elif res.error_code in (GroupCoordinatorNotAvailable.ERROR_CODE,
                                NotCoordinatorForGroup.ERROR_CODE):
            self._group_coordinator = self._cluster.get_group_coordinator(
                self._consumer_group)
        elif res.error_code == GroupAuthorizationFailed.ERROR_CODE:
            raise GroupAuthorizationFailed()
        self._rebalance()

    def _decide_partitions(self, participants, consumer_id=None):
        """Decide which partitions belong to this consumer

        Thin formatting wrapper around
        `pykafka.balancedconsumer.BalancedConsumer._decide_partitions`
        """
        parts = super(ManagedBalancedConsumer, self)._decide_partitions(
            participants, consumer_id=consumer_id)
        return [p.id for p in parts]

    def _update_member_assignment(self):
        """Join a managed consumer group and start consuming assigned partitions

        Equivalent to
        `pykafka.balancedconsumer.BalancedConsumer._update_member_assignment`,
        but uses the Kafka 0.9 Group Membership API instead of ZooKeeper to manage
        group state
        """
        for i in range(self._rebalance_max_retries):
            try:
                # send the initial JoinGroupRequest. Assigns a member id and tells the
                # coordinator about this consumer.
                join_result = self._group_coordinator.join_managed_consumer_group(
                    self._consumer_group, self._consumer_id)
                self._generation_id = join_result.generation_id
                self._consumer_id = join_result.member_id

                # generate partition assignments for each group member
                # if this is not the leader, join_result.members will be empty
                group_assignments = [
                    MemberAssignment([
                        (self._topic.name,
                            self._decide_partitions(iterkeys(join_result.members),
                                                    consumer_id=member_id))
                    ], member_id=member_id) for member_id in join_result.members]

                # perform the SyncGroupRequest. If this consumer is the group leader,
                # This request informs the other members of the group of their
                # partition assignments. This request is also used to fetch The
                # partition assignment for this consumer. The group leader *could*
                # tell itself its own assignment instead of using the result of this
                # request, but it does the latter to ensure consistency.
                sync_result = self._group_coordinator.sync_group(
                    self._consumer_group, self._generation_id, self._consumer_id,
                    group_assignments)
                assignment = sync_result.member_assignment.partition_assignment
                # set up a SimpleConsumer consuming the returned partitions
                my_partitions = [p for p in itervalues(self._topic.partitions)
                                 if p.id in assignment[0][1]]
                self._setup_internal_consumer(partitions=my_partitions)
                break
            except Exception:
                if i == self._rebalance_max_retries - 1:
                    log.warning('Failed to rebalance s after %d retries.', i)
                    raise
                log.info('Unable to complete rebalancing. Retrying')
                self._cluster.handler.sleep(i * (self._rebalance_backoff_ms / 1000))
        self._raise_worker_exceptions()
