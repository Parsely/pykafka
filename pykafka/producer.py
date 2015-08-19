from __future__ import division
"""
Author: Emmett Butler, Keith Bourgoin
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
__all__ = ["Producer", "SynchronousProducer"]
from collections import defaultdict, deque
import itertools
import logging
import sys
import time
import traceback

from .common import CompressionType
from .exceptions import (
    InvalidMessageError,
    InvalidMessageSize,
    MessageSizeTooLarge,
    NotLeaderForPartition,
    ProduceFailureError,
    ProducerQueueFullError,
    ProducerStoppedException,
    RequestTimedOut,
    SocketDisconnectedError,
    UnknownTopicOrPartition
)
from .partitioners import random_partitioner
from .protocol import Message, ProduceRequest


log = logging.getLogger(__name__)


class Producer(object):
    """
    This class implements asynchronous producer logic similar to that found in
    the JVM driver.

    It creates a thread of execution for each broker that is the leader of
    one or more of its topic's partitions. Each of these threads (which may
    use `threading` or some other parallelism implementation like `gevent`)
    is associated with a queue that holds the messages that are waiting to be
    sent to that queue's broker.
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
        :param required_acks: The number of other brokers that must have
            committed the data to their log and acknowledged this to the leader
            before a request is considered complete
        :type required_acks: int
        :param ack_timeout_ms: The amount of time (in milliseconds) to wait for
            acknowledgment of a produce request.
        :type ack_timeout_ms: int
        :param max_queued_messages: The maximum number of messages the producer
            can have waiting to be sent to the broker. If messages are sent
            faster than they can be delivered to the broker, the producer will
            either block or throw an exception based on the preference specified
            with block_on_queue_full.
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
            If False, we should throw an error immediately.
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
        self._worker_exception = None
        self._worker_trace_logged = False
        self._running = False
        self.start()

    def raise_worker_exceptions(fn):
        """Decorator that raises exceptions encountered on worker threads
        """
        def wrapped(self, *args, **kwargs):
            retval = fn(self, *args, **kwargs)
            if self._worker_exception is not None:
                _, ex, tb = self._worker_exception
                # avoid logging worker exceptions more than once, which can
                # happen when this function's `raise` triggers `__exit__`
                # which calls `stop`
                if not self._worker_trace_logged:
                    self._worker_trace_logged = True
                    log.error("Exception encountered in worker thread:\n%s",
                              "".join(traceback.format_tb(tb)))
                raise ex
            return retval
        return wrapped

    def __repr__(self):
        return "<{module}.{name} at {id_}>".format(
            module=self.__class__.__module__,
            name=self.__class__.__name__,
            id_=hex(id(self))
        )

    def __enter__(self):
        """Context manager entry point - start the producer"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point - stop the producer

        If __exit__ is being called as a result of self._worker_exception
        being raised, don't wait on the workers to finish. If __exit__ is being
        called because the with: block has ended, do wait for workers to finish.
        """
        # If the thread crashed, don't wait for it
        self.stop(wait=self._worker_exception is None)

    @raise_worker_exceptions
    def start(self):
        """Set up data structures and start worker threads"""
        if not self._running:
            self._owned_brokers = {}
            for partition in self._topic.partitions.values():
                if partition.leader.id not in self._owned_brokers:
                    self._owned_brokers[partition.leader.id] = OwnedBroker(
                        self, partition.leader)
            self._running = True

    @raise_worker_exceptions
    def stop(self, wait=True):
        """Mark the producer as stopped

        :param wait: Whether to wait for all inflight messages to be sent
            before returning
        :type wait: bool
        """
        self._running = False
        if wait:
            self._wait_all()

    @raise_worker_exceptions
    def produce(self, messages):
        """Produce a set of messages.

        :param messages: The messages to produce
        :type messages: Iterable of str or (str, str) tuples
        """
        def _partition_messages():
            """Assign messages to partitions using the partitioner."""
            partitions = self._topic.partitions.values()
            for message in messages:
                if isinstance(message, basestring):
                    key = None
                    value = message
                else:
                    key, value = message
                yield (key, str(value)), self._partitioner(partitions, message).id
        if not self._running:
            raise ProducerStoppedException()
        self._produce(_partition_messages())

    @raise_worker_exceptions
    def _produce(self, message_partition_tups):
        """Enqueue a set of messages for the relevant brokers

        :param message_partition_tups: Messages with partitions assigned.
        :type message_partition_tups: Iterable of ((str, str), int) tuples
        """
        for kv, partition_id in message_partition_tups:
            leader_id = self._topic.partitions[partition_id].leader.id
            self._owned_brokers[leader_id].enqueue([(kv, partition_id)])

    @raise_worker_exceptions
    def _prepare_request(self, message_batch, owned_broker):
        """Prepare a request and send it to the broker

        :param message_batch: An iterable of messages to send
        :type message_batch: iterable of `((key, value), partition_id)` tuples
        :param owned_broker: The `OwnedBroker` to which to send the request
        :type owned_broker: :class:`pykafka.producer.OwnedBroker`
        """
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
        log.debug("Sending %d messages to broker %d",
                  len(message_batch), owned_broker.broker.id)
        self._send_request(request, 0, owned_broker)

    @raise_worker_exceptions
    def _send_request(self, req, attempt, owned_broker):
        """Send the produce request to the broker and handle the response.

        :param req: The produce request to send
        :type req: :class:`pykafka.protocol.ProduceRequest`
        :param attempt: The current attempt count. Used for retry logic
        :type attempt: int
        :param owned_broker: The broker to which to send the request
        :type owned_broker: :class:`pykafka.producer.OwnedBroker`
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
            response = owned_broker.broker.produce_messages(req)

            # Figure out if we need to retry any messages
            # TODO: Convert to using utils.handle_partition_responses
            to_retry = []
            for topic, partitions in response.topics.iteritems():
                for partition, presponse in partitions.iteritems():
                    if presponse.err == 0:
                        # mark msg_count messages as successfully delivered
                        msg_count = len(req.msets[topic][partition].messages)
                        with owned_broker.lock:
                            owned_broker.messages_inflight -= msg_count
                        continue  # All's well
                    if presponse.err == UnknownTopicOrPartition.ERROR_CODE:
                        log.warning('Unknown topic: %s or partition: %s. '
                                    'Retrying.', topic, partition)
                    elif presponse.err == NotLeaderForPartition.ERROR_CODE:
                        log.warning('Partition leader for %s/%s changed. '
                                    'Retrying.', topic, partition)
                        # Update cluster metadata to get new leader
                        self._cluster.update()
                        self._update_leaders()
                    elif presponse.err == RequestTimedOut.ERROR_CODE:
                        log.warning('Produce request to %s:%s timed out. '
                                    'Retrying.', owned_broker.broker.host,
                                    owned_broker.broker.port)
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
                        owned_broker.broker.host,
                        owned_broker.broker.port)
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

    @raise_worker_exceptions
    def _update_leaders(self):
        """Ensure each message in each queue is in the queue owned by its
            partition's leader

        This function empties all broker queues, maps their messages to the
        current leaders for their partitions, and enqueues the messages in
        the appropriate queues.
        """
        # empty queues and figure out updated partition leaders
        new_queue_contents = defaultdict(list)
        for owned_broker in self._owned_brokers.itervalues():
            owned_broker.lock.acquire()
            current_queue_contents = owned_broker.flush(0, acquire=False,
                                                        release_inflight=True)
            for kv, partition_id in current_queue_contents:
                partition_leader = self._topic.partitions[partition_id].leader
                new_queue_contents[partition_leader.id].append((kv, partition_id))
        # retain locks for all brokers between these two steps
        for owned_broker in self._owned_brokers.itervalues():
            owned_broker.enqueue(new_queue_contents[owned_broker.broker.id],
                                 self._block_on_queue_full,
                                 acquire=False)
            owned_broker.resolve_event_state()
            owned_broker.lock.release()

    @raise_worker_exceptions
    def _wait_all(self):
        """Block until all messages in flight are sent

        "In flight" messages are those that have been used in calls to `produce`
        and have not yet been dequeued and sent to the broker
        """
        log.info("Blocking until all messages are sent")
        while any(q.message_is_inflight() for q in self._owned_brokers.itervalues()):
            time.sleep(.3)


class SynchronousProducer(Producer):
    """ This class implements the synchronous producer logic found in the JVM driver.
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
                 max_queued_messages=10000,
                 linger_ms=0,
                 block_on_queue_full=True):
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
        super(SynchronousProducer, self).__init__(
            cluster,
            topic,
            partitioner=partitioner,
            compression=compression,
            max_retries=max_retries,
            retry_backoff_ms=retry_backoff_ms,
            required_acks=required_acks,
            ack_timeout_ms=ack_timeout_ms,
            max_queued_messages=max_queued_messages,
            linger_ms=linger_ms,
            block_on_queue_full=block_on_queue_full
        )

    def produce(self, messages):
        """Produce a set of messages.

        :param messages: The messages to produce
        :type messages: Iterable of str or (str, str) tuples
        """
        super(SynchronousProducer, self).produce(messages)
        self._wait_all()


class OwnedBroker(object):
    """An abstraction over a broker connected to by the producer

    An OwnedBroker object contains thread-synchronization primitives
    and message queue corresponding to a single broker for this producer.

    :ivar lock: The lock used to control access to shared resources for this
        queue
    :ivar flush_ready: A condition variable that indicates that the queue is
        ready to be flushed via requests to the broker
    :ivar slot_available: A condition variable that indicates that there is
        at least one position free in the queue for a new message
    :ivar queue: The message queue for this broker. Contains messages that have
        been supplied as arguments to `produce()` waiting to be sent to the
        broker.
    :type queue: collections.deque
    :ivar messages_inflight: A counter indicating how many messages have been
        enqueued for this broker and not yet sent in a request.
    :type messages_inflight: int
    :ivar producer: The producer to which this OwnedBroker instance belongs
    :type producer: :class:`pykafka.producer.AsyncProducer`
    """
    def __init__(self, producer, broker):
        self.producer = producer
        self.broker = broker
        self.lock = self.producer._cluster.handler.Lock()
        self.flush_ready = self.producer._cluster.handler.Event()
        self.slot_available = self.producer._cluster.handler.Event()
        self.queue = deque()
        self.messages_inflight = 0

        def queue_reader():
            while True:
                try:
                    batch = self.flush(self.producer._linger_ms)
                    if batch:
                        self.producer._prepare_request(batch, self)
                except Exception:
                    # surface all exceptions to the main thread
                    self.producer._worker_exception = sys.exc_info()
                    break
        log.info("Starting new produce worker for broker %s", broker.id)
        self.producer._cluster.handler.spawn(queue_reader)

    def message_is_inflight(self):
        """
        Indicates whether there are currently any messages that have been
            `produce()`d and not yet sent to the broker
        """
        return self.messages_inflight > 0

    def enqueue(self, messages, acquire=True):
        """Push messages onto the queue

        :param messages: The messages to push onto the queue
        :type messages: iterable of tuples of the form
            `((key, value), partition_id)`
        :param acquire: Whether to acquire and release the lock. If False,
            this function assumes that the lock is already held by the caller
            and raises an AssertionError if not.
        :type acquire: bool
        """
        assert acquire or self.lock.locked()
        self._wait_for_slot_available(acquire=acquire)
        if acquire:
            self.lock.acquire()
        self.queue.extendleft(messages)
        self.messages_inflight += len(messages)
        if len(self.queue) >= self.producer._max_queued_messages:
            if not self.flush_ready.is_set():
                self.flush_ready.set()
        if acquire:
            self.lock.release()

    def flush(self, linger_ms, acquire=True, release_inflight=False):
        """Pop messages from the end of the queue

        :param linger_ms: How long (in milliseconds) to wait for the queue
            to contain messages before flushing
        :type linger_ms: int
        :param acquire: Whether to acquire and release the lock. If False,
            this function assumes that the lock is already held by the caller
            and raises an AssertionError if not.
        :type acquire: bool
        :param release_inflight: Whether to decrement the messages_inflight
            counter when the queue is flushed. True means that the messages
            popped from the queue will be discarded unless re-enqueued
            by the caller.
        :type release_inflight: bool
        """
        assert acquire or self.lock.locked()
        self._wait_for_flush_ready(linger_ms, acquire=acquire)
        if acquire:
            self.lock.acquire()
        batch = [self.queue.pop() for _ in xrange(len(self.queue))]
        if release_inflight:
            self.messages_inflight -= len(batch)
        if not self.slot_available.is_set():
            self.slot_available.set()
        if acquire:
            self.lock.release()
        return batch

    def _wait_for_flush_ready(self, linger_ms, acquire=True):
        """Block until the queue is ready to be flushed

        If the queue does not contain at least one message after blocking for
        `linger_ms` milliseconds, return.

        :param linger_ms: How long (in milliseconds) to wait for the queue
            to contain messages before returning
        :type linger_ms: int
        :param acquire: Whether to acquire and release the lock. If False,
            this function assumes that the lock is already held by the caller
            and raises an AssertionError if not.
        :type acquire: bool
        """
        assert acquire or self.lock.locked()
        if len(self.queue) == 0:
            if acquire:
                self.lock.acquire()
            if len(self.queue) == 0:
                self.flush_ready.clear()
            if acquire:
                self.lock.release()
            self.flush_ready.wait(linger_ms / 1000)

    def _wait_for_slot_available(self, acquire=True):
        """Block until the queue has at least one slot not containing a message

        :param acquire: Whether to acquire and release the lock. If False,
            this function assumes that the lock is already held by the caller
            and raises an AssertionError if not.
        :type acquire: bool
        """
        assert acquire or self.lock.locked()
        if len(self.queue) >= self.producer._max_queued_messages:
            if acquire:
                self.lock.acquire()
            if len(self.queue) >= self.producer._max_queued_messages:
                self.slot_available.clear()
            if acquire:
                self.lock.release()
            if self.producer._block_on_queue_full:
                self.slot_available.wait()
            else:
                raise ProducerQueueFullError("Queue full for broker %d",
                                             self.broker.id)

    def resolve_event_state(self):
        """Invariants for the Event variables used for thread synchronization
        """
        if len(self.queue) >= self.producer._max_queued_messages:
            self.slot_available.clear()
            self.flush_ready.set()
        elif len(self.queue) == 0:
            self.slot_available.set()
            self.flush_ready.clear()
        else:
            self.slot_available.set()
