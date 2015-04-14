import inspect
import logging
import time
from collections import defaultdict

from pykafka import base
from pykafka.common import CompressionType
from pykafka.exceptions import (
    UnknownTopicOrPartition, LeaderNotAvailable,
    NotLeaderForPartition, RequestTimedOut,
)
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

    def __init__(self, *args, **kwargs):
        """ For argspec see base.BaseProducer.__init__ """
        callargs = inspect.getcallargs(
                base.BaseProducer.__init__, self, *args, **kwargs)
        # Save each callarg as "_callarg" on self:
        del callargs["self"]
        map(lambda arg: setattr(self, "_" + arg[0], arg[1]),
            callargs.iteritems())

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
