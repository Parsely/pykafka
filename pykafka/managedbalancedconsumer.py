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
from .exceptions import IllegalGeneration, RebalanceInProgress, UnknownMemberId
from .protocol import MemberAssignment
from .utils.compat import itervalues, iterkeys

log = logging.getLogger(__name__)


class ManagedBalancedConsumer(BalancedConsumer):
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
                 compacted_topic=True,
                 heartbeat_interval_ms=3000):
        """A self-balancing consumer that uses Kafka 0.9's Group Membership API

        Implements the Group Management API semantics for Kafka 0.9 compatibility

        This class overrides the functionality of
        :class:`pykafka.balancedconsumer.BalancedConsumer` that deals with ZooKeeper and
        inherits other functionality directly.
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
        self._use_rdkafka = False
        self._running = True

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
            self._consumer_group,
            self._generation_id,
            self._consumer_id
        )
        if res.error_code == IllegalGeneration.ERROR_CODE:
            self._rebalance()
        elif res.error_code == RebalanceInProgress.ERROR_CODE:
            self._rebalance()
        elif res.error_code == UnknownMemberId.ERROR_CODE:
            self._rebalance()

    def _decide_partitions(self, participants, consumer_id=None):
        """Decide which partitions belong to this consumer

        Thin formatting wrapper around
        `pykafka.balancedconsumer.BalancedConsumer._decide_partitions`
        """
        parts = super(ManagedBalancedConsumer, self)._decide_partitions(
            participants, consumer_id=consumer_id)
        return [p.id for p in parts]

    def _rebalance(self):
        """Join a managed consumer group and start consuming assigned partitions

        Equivalent to `pykafka.balancedconsumer.BalancedConsumer._rebalance`,
        but uses the Kafka 0.9 Group Membership API instead of ZooKeeper to manage
        group state
        """
        if self._consumer is not None:
            self.commit_offsets()

        with self._rebalancing_lock:
            # send the initial JoinGroupRequest. Assigns a member id and tells the
            # coordinator about this consumer.
            join_result = self._group_coordinator.join_managed_consumer_group(
                self._consumer_group, self._consumer_id)
            self._generation_id = join_result.generation_id
            self._consumer_id = join_result.member_id

            # generate partition assignments for each group member
            # if this consumer is not the leader, join_result.members will be empty
            group_assignments = [
                MemberAssignment([
                    (self._topic.name,
                     self._decide_partitions(iterkeys(join_result.members),
                                             consumer_id=member_id))
                ], member_id=member_id) for member_id in join_result.members]

            # perform the SyncGroupRequest. If this consumer is the group leader, This
            # request informs the other members of the group of their partition
            # assignments. This request is also used to fetch the partition assignment
            # for this consumer. The group leader *could* tell itself its own assignment
            # instead of using the result of this request, but it does the latter to
            # ensure consistency.
            sync_result = self._group_coordinator.sync_group(self._consumer_group,
                                                             self._generation_id,
                                                             self._consumer_id,
                                                             group_assignments)
            my_partition_ids = sync_result.member_assignment.partition_assignment[0][1]
            # set up a SimpleConsumer consuming the returned partitions
            my_partitions = [p for p in itervalues(self._topic.partitions)
                             if p.id in my_partition_ids]
            self._setup_internal_consumer(partitions=my_partitions)
        self._raise_worker_exceptions()
