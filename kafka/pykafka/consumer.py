import itertools
from collections import defaultdict
import time
from Queue import Queue, Empty

from kafka import base
from kafka.common import OffsetType

from .protocol import PartitionFetchRequest

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
                 socket_timeout_ms=30000,
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

        :param topic: the topic this consumer should consume
        :type topic: pykafka.topic.Topic
        :param client_id:
        :type client_id:
        :param consumer_group: the name of the consumer group to join
        :type consumer_group: str
        :param partitions: existing partitions to which to connect
        :type partitions: list of pykafka.partition.Partition
        :param socket_timeout_ms: the socket timeout for network requests
        :type socket_timeout_ms: int
        :param socket_receive_buffer_bytes: the size of the socket receive
            buffer for network requests
        :type socket_receive_buffer_bytes: int
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
        :param queued_max_message_chunks: max number of message chunks buffered
            for consumption
        :type queued_max_message_chunks: int
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
        :param offsets_channel_socket_timeout_ms: socket timeout to use when
            reading responses for Offset Fetch/Commit requests. This timeout
            will also be used for the ConsumerMetdata requests that are used
            to query for the offset coordinator.
        :type offsets_channel_socket_timeout_ms: int
        :param offsets_commit_max_retries: Retry the offset commit up to this
            many times on failure.
        :type offsets_commit_max_retries: int
        :param auto_offset_reset: what to do if an offset is out of range
        :type auto_offset_reset: int
        :param consumer_timeout_ms: throw a timeout exception to the consumer
            if no message is available for consumption after the specified interval
        :type consumer_timeout_ms: int
        """
        self._consumer_group = consumer_group
        self._topic = topic
        self._fetch_message_max_bytes = fetch_message_max_bytes
        self._socket_timeout_ms = socket_timeout_ms

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._last_auto_commit = time.time()

        if partitions:
            self._partitions = {OwnedPartition(p, self): topic.partitons[p]
                                for p in partitions}
        else:
            self._partitions = {OwnedPartition(p, self): p
                                for k, p in topic.partitions.iteritems()}
        # Organize partitions by leader for efficient queries
        self._partitions_by_leader = defaultdict(list)
        for p in self._partitions.iterkeys():
            self._partitions_by_leader[p.partition.leader] = p
        self.partition_cycle = itertools.cycle(self._partitions.keys())

    @property
    def topic(self):
        return self._topic

    @property
    def partitions(self):
        return self._partitions

    @property
    def fetch_message_max_bytes(self):
        return self._fetch_message_max_bytes

    def __iter__(self):
        while True:
            yield self.consume(timeout=self._socket_timeout_ms)

    def consume(self, timeout=None):
        """Get one message from the consumer.

        :param timeout: Seconds to wait before returning None
        """
        owned_partition = self.partition_cycle.next()
        message = owned_partition.consume(timeout=timeout)

        if self._auto_commit_enable:
            self._auto_commit()

        return message

    def _auto_commit(self):
        if not self._auto_commit_enable or self._auto_commit_interval_ms == 0:
            return

        if (time.time() - self._last_auto_commit) * 1000.0 >= self._auto_commit_interval_ms:
            self.commit_offsets()

    def commit_offsets(self):
        """Use the Offset Commit/Fetch API to commit offsets for this
            consumer's topic
        """
        if not self.consumer_group:
            raise Exception("consumer group must be specified to commit offsets")

        self._last_auto_commit = time.time()

        for broker, partitions in self._partitions_by_leader:
            # XXX create a bunch of PartitionOffsetCommitRequests
            reqs = [p for p in partitions]
            broker.commit_offsets(self.consumer_group, reqs)

    def fetch_offsets(self):
        """Use the Offset Commit/Fetch API to fetch offsets for this
            consumer's topic
        """
        pass


class OwnedPartition(object):
    """A partition that is owned by a SimpleConsumer.

    Used to keep track of offsets and the internal message queue.
    """

    def __init__(self, partition, consumer):
        self.partition = partition
        self.consumer = consumer
        self._messages = Queue()
        self.last_offset_consumed = 0
        self.next_offset = 0

        if self.consumer._auto_commit_enable and self.consumer.consumer_group is not None:
            self.last_offset_consumed = self._fetch_last_known_offset()

    def consume(self, timeout=None):
        """Get a single message from this partition
        """
        if self._messages.empty():
            self._fetch(timeout=timeout)

        try:
            message = self._messages.get_nowait()
            self.last_offset_consumed = message.offset
            return message
        except Empty:
            return None

    def _fetch_last_known_offset(self):
        """Use the Offset Commit/Fetch API to find the last known offset for
            this partition
        """
        pass

    def _fetch(self, timeout=None):
        topic_name = self.partition.topic.name
        success = False
        while success is False:
            try:
                request = PartitionFetchRequest(
                    self.partition.topic.name, self.partition.id, self.next_offset,
                    self.consumer.fetch_message_max_bytes
                )
                response = self.partition.leader.fetch_messages([request],
                                                                timeout=timeout)

                messages = response.topics[topic_name].messages

                for message in messages:
                    if message.offset < self.last_offset_consumed:
                        continue
                    self._messages.put(message)
                    self.next_offset = message.offset + 1

                success = True
            except:
                success = False
