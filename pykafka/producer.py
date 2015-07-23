from __future__ import division
"""
Author: Keith Bourgoin
"""
__license__ = """
Copyright 2015 Parse.ly, Inc.

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
__all__ = ["Producer", "AsyncProducer"]
import itertools
import logging
import time
from collections import defaultdict
from Queue import Queue, Empty

from .common import CompressionType
from .exceptions import (
    UnknownTopicOrPartition, NotLeaderForPartition, RequestTimedOut,
    ProduceFailureError, SocketDisconnectedError, InvalidMessageError,
    InvalidMessageSize, MessageSizeTooLarge
)
from .partitioners import random_partitioner
from .protocol import Message, ProduceRequest


log = logging.getLogger(__name__)


class Producer():
    """
    This class implements the synchronous producer logic found in the
    JVM driver.
    """
    def __init__(self,
                 cluster,
                 topic,
                 partitioner=random_partitioner,
                 compression=CompressionType.NONE,
                 max_retries=3,
                 retry_backoff_ms=100,
                 required_acks=1,
                 ack_timeout_ms=10000,
                 batch_size=200):
        """Instantiate a new Producer.

        :param cluster: The cluster to which to connect
        :type cluster: :class:`pykafka.cluster.Cluster`
        :param topic: The topic to which to produce messages
        :type topic: :class:`pykafka.topic.Topic`
        :param partitioner: The partitioner to use during message production
        :type partitioner: :class:`pykafka.partitioners.BasePartitioner`
        :param compression: The type of compression to use.
        :type compression: :class:`pykafka.common.CompressionType`
        :param max_retries: How many times to attempt to produce messages
            before raising an error.
        :type max_retries: int
        :param retry_backoff_ms: The amount of time (in milliseconds) to
            back off during produce request retries.
        :type retry_backoff_ms: int
        :param required_acks: How many other brokers must have committed the
            data to their log and acknowledged this to the leader before a
            request is considered complete?
        :type required_acks: int
        :param ack_timeout_ms: Amount of time (in milliseconds) to wait for
            acknowledgment of a produce request.
        :type ack_timeout_ms: int
        :param batch_size: Size (in bytes) of batches to send to brokers.
        :type batch_size: int
        """
        # See BaseProduce.__init__.__doc__ for docstring
        self._cluster = cluster
        self._topic = topic
        self._partitioner = partitioner
        self._compression = compression
        self._max_retries = max_retries
        self._retry_backoff_ms = retry_backoff_ms
        self._required_acks = required_acks
        self._ack_timeout_ms = ack_timeout_ms
        self._batch_size = batch_size

    def __repr__(self):
        return "<{module}.{name} at {id_}>".format(
            module=self.__class__.__module__,
            name=self.__class__.__name__,
            id_=hex(id(self))
        )

    def _send_request(self, broker, req, attempt):
        """Send the produce request to the broker and handle the response.

        :param broker: The broker to which to send the request
        :type broker: :class:`pykafka.broker.Broker`
        :param req: The produce request to send
        :type req: :class:`pykafka.protocol.ProduceRequest`
        :param attempt: The current attempt count. Used for retry logic
        :type attempt: int
        """

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
            # TODO: Convert to using utils.handle_partition_responses
            to_retry = []
            for topic, partitions in response.topics.iteritems():
                for partition, presponse in partitions.iteritems():
                    if presponse.err == 0:
                        continue  # All's well
                    if presponse.err == UnknownTopicOrPartition.ERROR_CODE:
                        log.warning('Unknown topic: %s or partition: %s. '
                                    'Retrying.', topic, partition)
                    elif presponse.err == NotLeaderForPartition.ERROR_CODE:
                        log.warning('Partition leader for %s/%s changed. '
                                    'Retrying.', topic, partition)
                        # Update cluster metadata to get new leader
                        self._cluster.update()
                    elif presponse.err == RequestTimedOut.ERROR_CODE:
                        log.warning('Produce request to %s:%s timed out. '
                                    'Retrying.', broker.host, broker.port)
                    elif presponse.err == InvalidMessageError.ERROR_CODE:
                        log.warning('Encountered InvalidMessageError')
                    elif presponse.err == InvalidMessageSize.ERROR_CODE:
                        log.warning('Encountered InvalidMessageSize')
                        continue
                    elif presponse.err == MessageSizeTooLarge.ERROR_CODE:
                        log.warning('Encountered MessageSizeTooLarge')
                        continue
                    to_retry.extend(_get_partition_msgs(partition, req))
        except SocketDisconnectedError:
            log.warning('Broker %s:%s disconnected. Retrying.',
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
                # we have to call _produce here since the broker/partition
                # target for each message may have changed
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
            value = str(value)
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
                self._prepare_request(leader,
                                      requests.pop(leader.id),
                                      attempt)

        # Send any still not sent
        for leader, req in requests.iteritems():
            self._prepare_request(leader, req, attempt)

    def _prepare_request(self, leader, request, attempt):
        """Callback called when a request is ready to be sent

        Immediately sends the request to `leader`

        :param leader: The broker to which to send the request
        :type leader: :class:`pykafka.broker.Broker`
        :param request: The request to send
        :type request: :class:`pykafka.protocol.ProduceRequest`
        :param attempt: The attempt number for this send attempt
        :type attempt: int
        """
        self._send_request(leader, request, attempt)

    def produce(self, messages):
        """Produce a set of messages.

        :param messages: The messages to produce
        :type messages: Iterable of str or (str, str) tuples
        """
        # Do partition distribution here. We need to be able to retry producing
        # only *some* messages when a leader changes. Therefore, we don't want
        # a random partition distribution changing that on the retry.
        self._produce(self._partition_messages(messages), 0)


class AsyncProducer(Producer):
    """
    This class implements the asynchronous producer logic found in the
    JVM driver. In inherits from the synchronous implementation.
    """
    def __init__(self, *args, **kwargs):
        """Instantiate a new AsyncProducer

        For argument documentation, see :class:`pykafka.producer.Producer`
        """
        super(AsyncProducer, self).__init__(*args, **kwargs)
        self._setup_workers()

    def _setup_workers(self):
        """Spawn workers for connected brokers

        Creates one :class:`queue.Queue` instance and one worker thread per
        broker. The queue holds requests waiting to be sent, and the worker
        thread reads the end of the queue and sends requests.
        """
        self._queues_by_leader = {}
        for partition in self._topic.partitions.values():
            if partition.leader.id not in self._queues_by_leader:
                self._queues_by_leader[partition.leader.id] = self._setup_queue(partition.leader)

    def _setup_queue(self, broker):
        """Spawn a worker for the given broker

        Creates a :class:`queue.Queue` instance and a worker thread for the
        given broker.

        :param broker: The broker for which to instantiate this queue and worker
        :type broker: :class:`pykafka.broker.Broker`
        """
        def worker():
            while True:
                time.sleep(.0001)
                try:
                    request, attempt = request_queue.get_nowait()
                except Empty:
                    continue
                self._send_request(broker, request, attempt)
        request_queue = Queue()
        log.info("Starting new produce worker thread for broker %s", broker.id)
        self._cluster.handler.spawn(worker)
        return request_queue

    def _prepare_request(self, leader, request, attempt):
        """Callback called when a request is ready to be sent

        Enqueues the request in the queue associated with `leader`

        :param leader: The broker to which to send the request
        :type leader: :class:`pykafka.broker.Broker`
        :param request: The request to send
        :type request: :class:`pykafka.protocol.ProduceRequest`
        :param attempt: The attempt number for this send attempt
        :type attempt: int
        """
        self._queues_by_leader[leader.id].put((request, attempt))
        log.debug("Enqueued %d messages for broker %s",
                  request.message_count(),
                  leader.id)
