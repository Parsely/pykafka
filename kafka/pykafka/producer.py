import logging
import time
from collections import defaultdict

from kafka import base
from kafka.common import CompressionType
from kafka.exceptions import (
    UnknownTopicOrPartition, LeaderNotAvailable,
    NotLeaderForPartition, RequestTimedOut,
)
from kafka.partitioners import random_partitioner
from .protocol import Message, ProduceRequest


logger = logging.getLogger(__name__)


class AsyncProducer(base.BaseAsyncProducer):

    def __init__(self,
                 topic,
                 partitioner=None,
                 compression=CompressionType.NONE,
                 max_retries=3,
                 retry_backoff_ms=100,
                 topic_refresh_interval_ms=600000,
                 required_acks=0,
                 ack_timeout_ms=10000,
                 batch_size=200,
                 batch_time_ms=5000,
                 max_pending_messages=10000):
        """Create an AsyncProducer for a topic.

        :param topic: The topic to produce messages for.
        :type topic: :class:`kafka.pykafka.topic.Topic`
        :para compression: Compression to use for messages.
        :type compression:
        :param max_retries: Number of times to retry sending messages.
        :param retry_backoff_ms: Interval to wait between retries
        :param topic_refresh_interval_ms: Time between queries to refresh
            metadata about the topic. The Producer will also refresh this data
            when the cluster changes (e.g. partitions missing, etc), but this
            is the interval for how often it actively polls for changes.
        """
        pass

class Producer(base.BaseProducer):

    def __init__(self,
                 client,
                 topic,
                 partitioner=random_partitioner,
                 compression=CompressionType.NONE,
                 max_retries=3,
                 retry_backoff_ms=100,
                 topic_refresh_interval_ms=600000,
                 required_acks=1,
                 ack_timeout_ms=10000,
                 batch_size=200):
        """Create a Producer for a topic.

        :param client: KafkaClient used to connect to cluster.
        :param topic: The topic to produce messages for.
        :type topic: :class:`kafka.pykafka.topic.Topic`
        :para compression: Compression to use for messages.
        :type compression: :class:`kafka.common.CompressionType`
        :param max_retries: Number of times to retry sending messages.
        :param retry_backoff_ms: Interval to wait between retries
        :param topic_refresh_interval_ms: Time between queries to refresh
            metadata about the topic. The Producer will also refresh this data
            when the cluster changes (e.g. partitions missing, etc), but this
            is the interval for how often it actively polls for changes.
        :param required_acks:
        :param ack_timeout_ms:
        :param batch_size: Size of batches to send to brokers
        """
        self._client = client
        self._topic = topic
        self._partitioner = partitioner
        self._batch_size = batch_size
        self._compression = compression
        self._max_retries = max_retries
        self._retry_backoff_ms = retry_backoff_ms
        self._topic_refresh_interval_ms = topic_refresh_interval_ms
        self._required_acks = required_acks
        self._ack_timeout_ms = ack_timeout_ms

    def _send_request(self, broker, req):
        tries = 0
        while tries < self._max_retries:
            try:
                broker.produce_messages(req)
                break
            except (UnknownTopicOrPartition , LeaderNotAvailable,
                    NotLeaderForPartition, RequestTimedOut) as ex:
                # FIXME: Not all messages will have failed. Only some on a
                #        bad partition. Retrying should reflect that. The
                #        fix needs to be in protocol.py as well since it
                #        only raises one exception no matter how many errors.
                tries += 1
                if tries >= self._max_retries:
                    raise
                elif isinstance(ex, UnknownTopicOrPartition):
                    logger.warning('Unknown topic: %s. Retrying.', self._topic)
                elif isinstance(ex, LeaderNotAvailable):
                    logger.warning('Partition leader unavailable. Retrying.')
                elif isinstance(ex, NotLeaderForPartition):
                    # Update cluster metadata and retry the produce request
                    # FIXME: Can this recurse infinitely?
                    # FIXME: This will cause a re-partitioning of messages
                    self._client.update()
                    self._produce(req.messages)
                elif isinstance(ex, RequestTimedOut):
                    logger.warning('Produce request timed out. Retrying.')

    def _produce(self, messages):
        """Publish a set of messages to relevant brokers."""
        # Requests grouped by broker
        requests = defaultdict(lambda: ProduceRequest(
            compression_type=self._compression,
            required_acks=self._required_acks,
            timeout=self._ack_timeout_ms
        ))

        partitions = self._topic.partitions.values()
        for message in messages:
            if isinstance(message, basestring):
                key = None
                value = message
            else:
                key, value = message
            partition = self._partitioner(partitions, key)
            requests[partition.leader].add_message(
                Message(value, partition_key=key),
                self._topic.name,
                partition.id
            )
            # Send requests at the batch size
            if requests[partition.leader].message_count() >= self._batch_size:
                self._send_request(partition.leader,
                                   requests.pop(partition.leader))

        # Send any still not sent
        for broker, req in requests.iteritems():
            self._send_request(broker, req)

    def produce(self, messages):
        self._produce(messages)
