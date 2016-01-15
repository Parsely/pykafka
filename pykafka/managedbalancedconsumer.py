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

from .common import OffsetType

log = logging.getLogger(__name__)


class ManagedBalancedConsumer(object):
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
                 reset_offset_on_start=False):
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
        self._running = False

        self._consumer = None
        self._consumer_id = None

        self._discover_group_coordinator()
        self._join_group()

        if auto_start is True:
            self.start()

    def _discover_group_coordinator(self):
        """Set the group coordinator for this consumer."""
        self._group_coordinator = self._cluster.get_group_coordinator(self._consumer_group)

    def _join_group(self):
        res = self._group_coordinator.join_managed_consumer_group(self._consumer_group)
