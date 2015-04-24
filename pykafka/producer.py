from __future__ import division

import itertools
import time
import logging
from collections import defaultdict

import base
from .common import CompressionType
from .exceptions import (
    UnknownTopicOrPartition, NotLeaderForPartition, RequestTimedOut,
    ProduceFailureError, SocketDisconnectedError, InvalidMessageError,
    InvalidMessageSize, MessageSizeTooLarge
)
from .partitioners import random_partitioner
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
                 cluster,
                 topic,
                 partitioner=random_partitioner,
                 compression=CompressionType.NONE,
                 max_retries=3,
                 retry_backoff_ms=100,
                 topic_refresh_interval_ms=600000,
                 required_acks=1,
                 ack_timeout_ms=10000,
                 batch_size=200):
        # See BaseProduce.__init__.__doc__ for docstring
        self._cluster = cluster
        self._topic = topic
        self._partitioner = partitioner
        self._compression = compression
        self._max_retries = max_retries
        self._retry_backoff_ms = retry_backoff_ms
        self._topic_refresh_interval_ms = topic_refresh_interval_ms
        self._required_acks = required_acks
        self._ack_timeout_ms = ack_timeout_ms
        self._batch_size = batch_size

    def _send_request(self, broker, req, attempt):
        """Send the request to the broker and handle the response."""

        def _get_partition_msgs(partition_id, req):
            """Get all the messages for the partitions from the request."""
            messages = itertools.chain.from_iterable(
                mset.messages
                for topic, partitions in req.msets.iteritems()
                for p_id, mset in partitions.iteritems()
                if p_id == partition_id
            )
            for message in messages:
                yield (message.partition_key, message.value), partition_id

        # Do the request
        to_retry = []
        try:
            response = broker.produce_messages(req)

            # Figure out if we need to retry any messages
            to_retry = []
            for topic, partitions in response.topics.iteritems():
                for partition, (error, offset) in partitions.iteritems():
                    if error == 0:
                        continue  # All's well
                    if error == UnknownTopicOrPartition.ERROR_CODE:
                        logger.warning('Unknown topic: %s or partition: %s. Retrying.',
                                       self._topic, partition)
                    elif error == NotLeaderForPartition.ERROR_CODE:
                        logger.warning('Partition leader for %s/%s changed. '
                                       'Retrying.', topic, partition)
                        # Update cluster metadata to get new leader
                        self._cluster.update()
                    elif error == RequestTimedOut.ERROR_CODE:
                        logger.warning('Produce request to %s:%s timed out. '
                                       'Retrying.', broker.host, broker.port)
                    elif error == InvalidMessageError.ERROR_CODE:
                        logger.warning('Encountered InvalidMessageError')
                    elif error == InvalidMessageSize.ERROR_CODE:
                        logger.warning('Encountered InvalidMessageSize')
                        continue
                    elif error == MessageSizeTooLarge.ERROR_CODE:
                        logger.warning('Encountered MessageSizeTooLarge')
                        continue
                    to_retry.extend(_get_partition_msgs(partition, req))
        except SocketDisconnectedError:
            logger.warning('Broker %s:%s disconnected. Retrying.',
                            broker.host, broker.port)
            self._cluster.update()
            to_retry = [
                ((message.partition_key, message.value), p_id)
                for topic, partitions in req.msets.iteritems()
                for p_id, mset in partitions.iteritems()
                for message in mset.messages
            ]

        if to_retry:
            attempt += 1
            if attempt < self._max_retries:
                time.sleep(self._retry_backoff_ms / 1000)
                self._produce(to_retry, attempt)
            else:
                raise ProduceFailureError('Unable to produce messages. See log for details.')

    def _partition_messages(self, messages):
        """Assign messages to partitions using the partitioner.

        :param messages: Iterable of messages to publish.
        :returns:        Generator of ((key, value), partition_id)
        """
        partitions = self._topic.partitions.values()
        for message in messages:
            if isinstance(message, basestring):
                key = None
                value = message
            else:
                key, value = message
            yield (key, value), self._partitioner(partitions, message).id

    def _produce(self, message_partition_tups, attempt):
        """Publish a set of messages to relevant brokers.

        :param message_partition_tups: Messages with partitions assigned.
        :type message_partition_tups:  tuples of ((key, value), partition_id)
        """
        # Requests grouped by broker
        requests = defaultdict(lambda: ProduceRequest(
            compression_type=self._compression,
            required_acks=self._required_acks,
            timeout=self._ack_timeout_ms,
        ))

        for ((key, value), partition_id) in message_partition_tups:
            # N.B. This handles retries, so the leader lookup is needed
            leader = self._topic.partitions[partition_id].leader
            requests[leader].add_message(
                Message(value, partition_key=key),
                self._topic.name,
                partition_id
            )
            # Send requests at the batch size
            if requests[leader].message_count() >= self._batch_size:
                self._send_request(leader,
                                   requests.pop(leader),
                                   attempt)

        # Send any still not sent
        for leader, req in requests.iteritems():
            self._send_request(leader, req, attempt)

    def produce(self, messages):
        # Do partition distribution here. We need to be able to retry producing
        # only *some* messages when a leader changes. Therefore, we don't want
        # a random partition distribution changing that on the retry.
        self._produce(self._partition_messages(messages), 0)
