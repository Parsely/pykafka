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
import itertools
import logging

from .balancedconsumer import BalancedConsumer
from .common import OffsetType
from .protocol import MemberAssignment
from .utils.compat import iteritems, itervalues

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
                 auto_start=True,
                 reset_offset_on_start=False,
                 compacted_topic=True):
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
        self._is_compacted_topic = compacted_topic
        self._use_rdkafka = False
        self._worker_exception = None
        self._running = False

        self._consumer = None
        self._consumer_id = None
        self._is_group_leader = False

        self._discover_group_coordinator()
        self._join_group()

    def _discover_group_coordinator(self):
        """Set the group coordinator for this consumer."""
        self._group_coordinator = self._cluster.get_group_coordinator(self._consumer_group)

    def _join_group(self):
        res = self._group_coordinator.join_managed_consumer_group(self._consumer_group)
        self._generation_id = res.generation_id
        self._consumer_id = res.member_id
        all_members = []
        if len(res.members) > 0:
            self._is_group_leader = True
            all_members = [member_id for member_id, _ in iteritems(res.members)]
        group_assignments = []
        leader_assignment = None
        for member_id, metadata in iteritems(res.members):
            partitions = self._decide_partitions(all_members, member_id)
            assignment = (self._topic.name, [p.id for p in partitions])
            if member_id == self._consumer_id and self._is_group_leader:
                leader_assignment = assignment
            group_assignments.append((member_id, MemberAssignment([assignment])))
        res = self._group_coordinator.sync_group(self._consumer_group,
                                                 self._generation_id,
                                                 self._consumer_id,
                                                 group_assignments)
        if self._is_group_leader:
            assignment = leader_assignment[1]
        else:
            assignment = res.member_assignment.partition_assignment[1]
        self._setup_internal_consumer(
            partitions=[p for p in itervalues(self._topic.partitions)
                        if p.id in assignment])

    def _decide_partitions(self, participants, member_id):
        # Freeze and sort partitions so we always have the same results
        p_to_str = lambda p: '-'.join([str(p.topic.name), str(p.leader.id), str(p.id)])
        all_parts = self._topic.partitions.values()
        all_parts = sorted(all_parts, key=p_to_str)

        # get start point, # of partitions, and remainder
        participants = sorted(participants)  # just make sure it's sorted.
        idx = participants.index(member_id)
        parts_per_consumer = len(all_parts) // len(participants)
        remainder_ppc = len(all_parts) % len(participants)

        start = parts_per_consumer * idx + min(idx, remainder_ppc)
        num_parts = parts_per_consumer + (0 if (idx + 1 > remainder_ppc) else 1)

        # assign partitions from i*N to (i+1)*N - 1 to consumer Ci
        new_partitions = itertools.islice(all_parts, start, start + num_parts)
        new_partitions = set(new_partitions)
        log.info('Balancing %i participants for %i partitions.\nOwning %i partitions.',
                 len(participants), len(all_parts), len(new_partitions))
        log.debug('My partitions: %s', [p_to_str(p) for p in new_partitions])
        return new_partitions
