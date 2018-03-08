from __future__ import division
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
import uuid
import weakref

from .balancedconsumer import BalancedConsumer
from .common import OffsetType
from .exceptions import (IllegalGeneration, RebalanceInProgress, NotCoordinatorForGroup,
                         GroupCoordinatorNotAvailable, ERROR_CODES, GroupLoadInProgress,
                         UnicodeException)
from .membershipprotocol import RangeProtocol
from .protocol import MemberAssignment
from .utils.compat import iterkeys, get_string
from .utils.error_handlers import valid_int

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
                 fetch_error_backoff_ms=500,
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
                 heartbeat_interval_ms=3000,
                 membership_protocol=RangeProtocol,
                 deserializer=None,
                 reset_offset_on_fetch=True):
        """Create a ManagedBalancedConsumer instance

        :param topic: The topic this consumer should consume
        :type topic: :class:`pykafka.topic.Topic`
        :param cluster: The cluster to which this consumer should connect
        :type cluster: :class:`pykafka.cluster.Cluster`
        :param consumer_group: The name of the consumer group this consumer
            should join. Consumer group names are namespaced at the cluster level,
            meaning that two consumers consuming different topics with the same group name
            will be treated as part of the same group.
        :type consumer_group: str
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
        :param fetch_error_backoff_ms: *UNUSED*.
            See :class:`pykafka.simpleconsumer.SimpleConsumer`.
        :type fetch_error_backoff_ms: int
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
            topics do not provide offsets in strict incrementing order.
        :type compacted_topic: bool
        :param heartbeat_interval_ms: The amount of time in milliseconds to wait between
            heartbeat requests
        :type heartbeat_interval_ms: int
        :param membership_protocol: The group membership protocol to which this consumer
            should adhere
        :type membership_protocol: :class:`pykafka.membershipprotocol.GroupMembershipProtocol`
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

        self._cluster = cluster
        try:
            self._consumer_group = get_string(consumer_group).encode('ascii')
        except UnicodeEncodeError:
            raise UnicodeException("Consumer group name '{}' contains non-ascii "
                                   "characters".format(consumer_group))
        self._topic = topic

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = valid_int(auto_commit_interval_ms)
        self._fetch_message_max_bytes = valid_int(fetch_message_max_bytes)
        self._fetch_min_bytes = valid_int(fetch_min_bytes)
        self._num_consumer_fetchers = valid_int(num_consumer_fetchers)
        self._queued_max_messages = valid_int(queued_max_messages)
        self._fetch_wait_max_ms = valid_int(fetch_wait_max_ms, allow_zero=True)
        self._consumer_timeout_ms = valid_int(consumer_timeout_ms,
                                              allow_zero=True, allow_negative=True)
        self._offsets_channel_backoff_ms = valid_int(offsets_channel_backoff_ms)
        self._offsets_commit_max_retries = valid_int(offsets_commit_max_retries,
                                                     allow_zero=True)
        self._auto_offset_reset = auto_offset_reset
        self._reset_offset_on_start = reset_offset_on_start
        self._rebalance_max_retries = valid_int(rebalance_max_retries, allow_zero=True)
        self._rebalance_backoff_ms = valid_int(rebalance_backoff_ms)
        self._post_rebalance_callback = post_rebalance_callback
        self._is_compacted_topic = compacted_topic
        self._membership_protocol = membership_protocol
        self._membership_protocol.metadata.topic_names = [self._topic.name]
        self._heartbeat_interval_ms = valid_int(heartbeat_interval_ms)
        self._deserializer = deserializer
        self._reset_offset_on_fetch = reset_offset_on_fetch
        if use_rdkafka is True:
            raise ImportError("use_rdkafka is not available for {}".format(
                self.__class__.__name__))
        self._use_rdkafka = use_rdkafka

        self._generation_id = -1
        self._rebalancing_lock = cluster.handler.Lock()
        self._rebalancing_in_progress = self._cluster.handler.Event()
        self._internal_consumer_running = self._cluster.handler.Event()
        # ManagedBalancedConsumers in the same process cannot share connections.
        # This connection hash is passed to Broker calls that use the group
        # membership  API
        self._connection_id = uuid.uuid4()
        self._consumer = None
        self._group_coordinator = None
        self._consumer_id = b''
        self._worker_exception = None
        self._default_error_handlers = self._build_default_error_handlers()

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

                    log.info("Sending heartbeat from consumer '%s'", self._consumer_id)
                    res = self._group_coordinator.heartbeat(self._connection_id,
                                                            self._consumer_group,
                                                            self._generation_id,
                                                            self._consumer_id)
                    if res.error_code != 0:
                        log.info("Error code %d encountered on heartbeat.",
                                 res.error_code)
                        self._handle_error(res.error_code)
                        self._rebalance()

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
        """Start this consumer.

        Must be called before consume() if `auto_start=False`.
        """
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
        if self._group_coordinator is not None:
            self._group_coordinator.leave_group(self._connection_id,
                                                self._consumer_group,
                                                self._consumer_id)

    def _update_member_assignment(self):
        """Join a managed consumer group and start consuming assigned partitions

        Equivalent to
        `pykafka.balancedconsumer.BalancedConsumer._update_member_assignment`,
        but uses the Kafka 0.9 Group Membership API instead of ZooKeeper to manage
        group state
        """
        for i in range(self._rebalance_max_retries):
            try:
                members = self._join_group()
                # generate partition assignments for each group member
                # if this is not the leader, join_result.members will be empty
                group_assignments = [
                    (member_id,
                     MemberAssignment([
                        (self._topic.name,
                         [p.id for p in self._membership_protocol.decide_partitions(
                          iterkeys(members), self._topic.partitions, member_id)])])
                    ) for member_id in members]

                assignment = self._sync_group(group_assignments)
                self._setup_internal_consumer(
                    partitions=[self._topic.partitions[pid] for pid in assignment[0][1]])
                log.debug("Successfully rebalanced consumer '%s'", self._consumer_id)
                break
            except Exception as ex:
                if i == self._rebalance_max_retries - 1:
                    log.warning('Failed to rebalance s after %d retries.', i)
                    raise
                log.exception(ex)
                log.info('Unable to complete rebalancing. Retrying')
                self._cluster.handler.sleep(i * (self._rebalance_backoff_ms / 1000))
        self._raise_worker_exceptions()

    def _build_default_error_handlers(self):
        """Set up default responses to common error codes"""
        self = weakref.proxy(self)

        def _handle_GroupCoordinatorNotAvailable():
            self._group_coordinator = self._cluster.get_group_coordinator(
                self._consumer_group)

        def _handle_NotCoordinatorForGroup():
            self._group_coordinator = self._cluster.get_group_coordinator(
                self._consumer_group)

        return {
            GroupCoordinatorNotAvailable.ERROR_CODE: _handle_GroupCoordinatorNotAvailable,
            NotCoordinatorForGroup.ERROR_CODE: _handle_NotCoordinatorForGroup,
            GroupLoadInProgress.ERROR_CODE: None,
            RebalanceInProgress.ERROR_CODE: None,
            IllegalGeneration.ERROR_CODE: None
        }

    def _handle_error(self, error_code):
        """Call the appropriate handler function for the given error code

        :param error_code: The error code returned from a Group Membership API request
        :type error_code: int
        """
        if error_code not in self._default_error_handlers:
            raise ERROR_CODES[error_code]()
        if self._default_error_handlers[error_code] is not None:
            self._default_error_handlers[error_code]()

    def _join_group(self):
        """Send a JoinGroupRequest.

        Assigns a member id and tells the coordinator about this consumer.
        """
        log.info("Sending JoinGroupRequest for consumer id '%s'", self._consumer_id)
        for i in range(self._cluster._max_connection_retries):
            join_result = self._group_coordinator.join_group(self._connection_id,
                                                             self._consumer_group,
                                                             self._consumer_id,
                                                             self._topic.name,
                                                             self._membership_protocol)
            if join_result.error_code == 0:
                break
            log.info("Error code %d encountered during JoinGroupRequest for"
                     " generation '%s'", join_result.error_code, self._generation_id)
            if i == self._cluster._max_connection_retries - 1:
                raise ERROR_CODES[join_result.error_code]
            self._handle_error(join_result.error_code)
            self._cluster.handler.sleep(i * 2)
        self._generation_id = join_result.generation_id
        self._consumer_id = join_result.member_id
        return join_result.members

    def _sync_group(self, group_assignments):
        """Send a SyncGroupRequest.

        If this consumer is the group leader, this call informs the other consumers
        of their partition assignments. For all consumers including the leader, this call
        is used to fetch partition assignments.

        The group leader *could* tell itself its own assignment instead of using the
        result of this request, but it does the latter to ensure consistency.
        """
        log.info("Sending SyncGroupRequest for consumer id '%s'", self._consumer_id)
        for i in range(self._cluster._max_connection_retries):
            sync_result = self._group_coordinator.sync_group(self._connection_id,
                                                             self._consumer_group,
                                                             self._generation_id,
                                                             self._consumer_id,
                                                             group_assignments)
            if sync_result.error_code == 0:
                break
            log.info("Error code %d encountered during SyncGroupRequest",
                     sync_result.error_code)
            if i == self._cluster._max_connection_retries - 1:
                raise ERROR_CODES[sync_result.error_code]
            self._handle_error(sync_result.error_code)
            self._cluster.handler.sleep(i * 2)
        return sync_result.member_assignment.partition_assignment
