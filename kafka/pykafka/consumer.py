import itertools
from collections import defaultdict
from Queue import Queue

from kafka import base
from kafka.common import OffsetType

# Settings to add to the eventual BalancedConsumer
# consumer_group
# rebalance_max_retries
# rebalance_backoff_ms
# partition_assignment_strategy


class SimpleConsumer(base.BaseSimpleConsumer):

    def __init__(self,
                 topic,
                 client_id=None,
                 consumer_group=None,
                 partitions=None,
                 socket_timout_ms=30000,
                 socket_receive_buffer_bytes=60 * 1024,
                 fetch_message_max_bytes=1024 * 1024,
                 num_consumer_fetchers=1,
                 auto_commit_enable=False,
                 auto_commit_interval_ms=60 * 1000,
                 queued_max_message_chunks=2,
                 fetch_min_bytes=1,
                 fetch_wait_max_ms=100,
                 refresh_leader_backoff_ms=200,
                 offsets_channel_backoff_ms=1000,
                 offsets_channel_socket_timeout_ms=10000,
                 offsets_commit_max_retries=5,
                 auto_offset_reset=OffsetType.LATEST,
                 consumer_timeout_ms=-1):
        """Create a SimpleConsumer.

        Settings and default values are taken from the Scala
        consumer implementation.  Consumer group is included
        because it's necessary for offset management, but doesn't imply
        that this is a balancing consumer. Use a BalancedConsumer for
        that.

        TODO: param docs
        """
        self._consumer_group = None
        self._topic = topic
        if partitions:
            self._partitions = {OwnedPartition(p): topic.partitons[p]
                                for p in partitions}
        else:
            self._partitons = topic.partitions.copy()
        # Organize partitions by broker for efficient queries
        self._partitions_by_broker = defaultdict(list)
        for p in self._partitions.itervalues():
            self._partitions_by_broker[p.broker] = p
        self.partition_cycle = itertools.cycle(self._partitions.values())

    @property
    def topic(self):
        return self._topic

    @property
    def partitions(self):
        return self._partitions

    def __iter__(self):
        while True:
            yield self.consume()

    def consume(self, timeout=None):
        """Get one message from the consumer.

        :param timeout: Seconds to wait before returning None
        """
        pass


class OwnedPartition(object):
    """A partition that is owned by a SimpleConsumer.

    Used to keep track of offsets and the internal message queue.
    """

    def __init__(self, partition):
        self.partition = partition
        self._messages = Queue()

    def consume(self, timeout=None):
        pass
