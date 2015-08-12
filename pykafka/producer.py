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
from collections import defaultdict, deque, namedtuple
import threading

from .common import CompressionType
from .exceptions import (
    UnknownTopicOrPartition, NotLeaderForPartition, RequestTimedOut,
    ProduceFailureError, SocketDisconnectedError, InvalidMessageError,
    InvalidMessageSize, MessageSizeTooLarge, ProducerQueueFullError
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

    def _send_request(self, broker, req, attempt, q_info=None):
        """Send the produce request to the broker and handle the response.

        :param broker: The broker to which to send the request
        :type broker: :class:`pykafka.broker.Broker`
        :param req: The produce request to send
        :type req: :class:`pykafka.protocol.ProduceRequest`
        :param attempt: The current attempt count. Used for retry logic
        :type attempt: int
        :param q_info: A QueueInfo namedtuple containing information
            about the queue from which the messages in `req` were removed
        :type q_info: :class:`pykafka.producer.QueueInfo`
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
                        if q_info is not None:
                            with q_info.lock:
                                q_info.messages_inflight -= 1
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
                self._send_request(leader, requests.pop(leader), attempt)

        # Send any still not sent
        for leader, req in requests.iteritems():
            self._send_request(leader, req, attempt)

    def produce(self, messages):
        """Produce a set of messages.

        :param messages: The messages to produce
        :type messages: Iterable of str or (str, str) tuples
        """
        # Do partition distribution here. We need to be able to retry producing
        # only *some* messages when a leader changes. Therefore, we don't want
        # a random partition distribution changing that on the retry.
        self._produce(self._partition_messages(messages), 0)


_QueueInfo = namedtuple('QueueInfo', ['lock', 'flush_ready',
                                      'slot_available', 'queue'])


class QueueInfo(_QueueInfo):
    """A packet of information for a producer queue

    A QueueInfo object contains thread-synchronization primitives corresponding
    to a single broker for this producer.

    :ivar lock: The lock used to control access to shared resources for this
        queue
    :type lock: threading.Lock
    :ivar flush_ready: A condition variable that indicates that the queue is
        ready to be flushed
    :type flush_ready: threading.Condition
    :ivar slot_available: A condition variable that indicates that there is
        at least one position free in the queue for a new message
    :type slot_available: threading.Condition
    :ivar queue: The message queue for this broker
    :type queue: collections.deque
    :ivar messages_inflight: A counter indicating how many messages have been
        enqueued for this broker and not yet sent in a request.
    :type messages_inflight: int
    """
    def __new__(cls, lock, flush_ready, slot_available, queue, messages_inflight):
        ins = super(QueueInfo, cls).__new__(
            cls, lock, flush_ready, slot_available, queue)
        # store messages_inflight separately to allow updating
        ins.messages_inflight = messages_inflight
        return ins


class AsyncProducer():
    """
    This class implements asynchronous producer logic similar to that found in
    the JVM driver. In inherits from the synchronous implementation.
    """
    def __init__(self,
                 cluster,
                 topic,
                 partitioner=random_partitioner,
                 compression=CompressionType.NONE,
                 max_retries=3,
                 retry_backoff_ms=100,
                 required_acks=1,
                 ack_timeout_ms=10 * 1000,
                 max_queued_messages=10000,
                 linger_ms=0,
                 block_on_queue_full=True):
        """Instantiate a new AsyncProducer

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
        :param max_queued_messages: The maximum number of messages the producer
            can have waiting to be sent to the broker. If messages are sent
            faster than they can be delivered to the broker, the producer will
            either block or throw an exception based on the preference specified
            by block_on_queue_full.
        :type max_queued_messages: int
        :param linger_ms: This setting gives the upper bound on the delay for
            batching: once the producer gets max_queued_messages worth of
            messages for a broker, it will be sent immediately regardless of
            this setting.  However, if we have fewer than this many messages
            accumulated for this partition we will 'linger' for the specified
            time waiting for more records to show up. linger_ms=0 indicates no
            lingering.
        :type linger_ms: int
        :param block_on_queue_full: When the producer's message queue for a
            broker contains max_queued_messages, we must either stop accepting
            new messages (block) or throw an error. If True, this setting
            indicates we should block until space is available in the queue.
        :type block_on_queue_full: bool
        """
        self._cluster = cluster
        self._topic = topic
        self._partitioner = partitioner
        self._compression = compression
        self._max_retries = max_retries
        self._retry_backoff_ms = retry_backoff_ms
        self._required_acks = required_acks
        self._ack_timeout_ms = ack_timeout_ms
        self._max_queued_messages = max_queued_messages
        self._linger_ms = linger_ms
        self._block_on_queue_full = block_on_queue_full

        self._running = False
        self.start()

    def start(self):
        if not self._running:
            self._setup_internal_producer()
            self._setup_workers()
            self._running = True

    def stop(self):
        self._running = False
        last_log = time.time()
        while any(q_info.messages_inflight
                  for leader, q_info in self._workers_by_leader.iteritems()):
            if time.time() - last_log >= .5:
                log.info("Waiting for all worker threads to exit")
                last_log = time.time()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def _setup_internal_producer(self):
        self._producer = Producer(
            self._cluster,
            self._topic,
            self._partitioner,
            compression=self._compression,
            max_retries=self._max_retries,
            retry_backoff_ms=self._retry_backoff_ms,
            required_acks=self._required_acks,
            ack_timeout_ms=self._ack_timeout_ms,
        )

    def _setup_workers(self):
        """Spawn workers for connected brokers

        Creates one :class:`queue.Queue` instance and one worker thread per
        broker. The queue holds requests waiting to be sent, and the worker
        thread reads the end of the queue and sends requests.
        """
        # _workers_by_leader maps each broker id to a QueueInfo instance
        # containing references to that broker's queue and its associated
        # synchronization primitives
        self._workers_by_leader = {}
        for partition in self._topic.partitions.values():
            if partition.leader.id not in self._workers_by_leader:
                q_info = self._setup_queue(partition.leader)
                self._workers_by_leader[partition.leader.id] = q_info

    def _produce(self, message_partition_tups):
        # group messages by destination broker
        messages_by_leader = defaultdict(list)
        for ((key, value), partition_id) in message_partition_tups:
            leader = self._topic.partitions[partition_id].leader
            messages_by_leader[leader.id].append(((key, value), partition_id))

        # enqueue messages in the appropriate queue
        for broker_id, messages in messages_by_leader.iteritems():
            # get the queue associated with this broker
            q_info = self._workers_by_leader[broker_id]
            if len(q_info.queue) >= self._max_queued_messages:
                with q_info.lock:
                    if len(q_info.queue) >= self._max_queued_messages:
                        q_info.slot_available.clear()
                if self._block_on_queue_full:
                    q_info.slot_available.wait()
                else:
                    raise ProducerQueueFullError("Queue full for broker %d",
                                                 broker_id)
            with q_info.lock:
                to_extend = messages[:self._max_queued_messages - len(q_info.queue)]
                for message in to_extend:
                    q_info.messages_inflight += 1
                q_info.queue.extendleft(to_extend)
                log.debug("Enqueued %d messages for broker %d",
                          len(messages), broker_id)
                # should only set this if queue is full or timeout
                if len(q_info.queue) >= self._max_queued_messages:
                    if not q_info.flush_ready.is_set():
                        q_info.flush_ready.set()

    def _setup_queue(self, broker):
        """Spawn a worker for the given broker

        Creates a :class:`queue.Queue` instance and a worker thread for the
        given broker.

        :param broker: The broker for which to instantiate this queue and worker
        :type broker: :class:`pykafka.broker.Broker`
        """
        # this lock is used to ensure only one thread is manipulating these
        # Events or the queue at a given moment
        lock = threading.Lock()
        # When flush_ready is set, the queue is ready to be flushed to the
        # broker
        flush_ready = threading.Event()
        # when slot_available is set, there is space in the queue for at least
        # one message
        slot_available = threading.Event()
        # this queue contains the messages that have been produced and are
        # waiting to be sent to the broker
        message_queue = deque()
        # counter that indicates the number of messages that have been
        # enqueued and not yet sent successfully.
        messages_inflight = 0

        q_info = QueueInfo(lock=lock,
                           flush_ready=flush_ready,
                           slot_available=slot_available,
                           queue=message_queue,
                           messages_inflight=messages_inflight)

        def queue_reader():
            while True:
                if len(message_queue) == 0:
                    with lock:
                        if len(message_queue) == 0:
                            flush_ready.clear()
                    flush_ready.wait(self._linger_ms / 1000)
                with lock:
                    batch = [message_queue.pop() for _ in xrange(len(message_queue))]
                    if not slot_available.is_set():
                        slot_available.set()
                # this check is necessary since it's possible to get here when
                # the queue is still empty (in cases where self._linger_ms is
                # reached)
                if batch:
                    self._send_request(batch, broker, q_info)

        self._cluster.handler.spawn(queue_reader)
        log.info("Starting new produce worker thread for broker %s", broker.id)
        return q_info

    def _send_request(self, message_batch, broker, q_info):
        """Prepare a request and send it to the broker

        :param message_batch: An iterable of messages to send
        :type message_batch: iterable of `((key, value), partition_id)` tuples
        :param broker: The broker to which to send the request
        :type broker: :class:`pykafka.broker.Broker`
        :param q_info: A namedtuple containing information about the queue
            from which the messages in `message_batch` were taken
        :type q_info: :class:`pykafka.producer.QueueInfo`
        """
        log.debug("Sending %d messages to broker %d", len(message_batch), broker.id)
        request = ProduceRequest(
            compression_type=self._compression,
            required_acks=self._required_acks,
            timeout=self._ack_timeout_ms
        )
        for (key, value), partition_id in message_batch:
            request.add_message(
                Message(value, partition_key=key),
                self._topic.name,
                partition_id
            )
        try:
            self._producer._send_request(broker, request, 0, q_info)
        except ProduceFailureError as e:
            log.error("Producer error: %s", e)

    def produce(self, messages):
        """Produce a set of messages.

        :param messages: The messages to produce
        :type messages: Iterable of str or (str, str) tuples
        """
        self._produce(self._producer._partition_messages(messages))
