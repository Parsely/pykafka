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
__all__ = ["Producer"]
from collections import deque
import logging
import platform
import struct
import sys
import threading
import time
import weakref

from six import reraise

from .common import CompressionType
from .exceptions import (
    ERROR_CODES,
    KafkaException,
    InvalidMessageSize,
    MessageSizeTooLarge,
    NotLeaderForPartition,
    ProduceFailureError,
    ProducerQueueFullError,
    ProducerStoppedException,
    SocketDisconnectedError,
)
from .partitioners import RandomPartitioner
from .protocol import Message, ProduceRequest
from .utils.compat import iteritems, itervalues, Empty
from .utils.error_handlers import valid_int
from .utils import msg_protocol_version

log = logging.getLogger(__name__)


class Producer(object):
    """Implements asynchronous producer logic similar to the JVM driver.

    It creates a thread of execution for each broker that is the leader of
    one or more of its topic's partitions. Each of these threads (which may
    use `threading` or some other parallelism implementation like `gevent`)
    is associated with a queue that holds the messages that are waiting to be
    sent to that queue's broker.
    """
    def __init__(self,
                 cluster,
                 topic,
                 partitioner=None,
                 compression=CompressionType.NONE,
                 max_retries=3,
                 retry_backoff_ms=100,
                 required_acks=1,
                 ack_timeout_ms=10 * 1000,
                 max_queued_messages=100000,
                 min_queued_messages=70000,
                 linger_ms=5 * 1000,
                 # XXX 0 default here mirrors previous behavior - should default have a
                 # nonzero wait to save CPU cycles?
                 queue_empty_timeout_ms=0,
                 block_on_queue_full=True,
                 max_request_size=1000012,
                 sync=False,
                 delivery_reports=False,
                 pending_timeout_ms=5 * 1000,
                 auto_start=True,
                 serializer=None):
        """Instantiate a new AsyncProducer

        :param cluster: The cluster to which to connect
        :type cluster: :class:`pykafka.cluster.Cluster`
        :param topic: The topic to which to produce messages
        :type topic: :class:`pykafka.topic.Topic`
        :param partitioner: The partitioner to use during message production
        :type partitioner: :class:`pykafka.partitioners.BasePartitioner`
        :param compression: The type of compression to use.
        :type compression: :class:`pykafka.common.CompressionType`
        :param max_retries: How many times to attempt to produce a given batch of
            messages before raising an error. Allowing retries will potentially change
            the ordering of records because if two records are sent to a single partition,
            and the first fails and is retried but the second succeeds, then the second
            record may appear first. If you want to completely disallow message
            reordering, use `sync=True`.
        :type max_retries: int
        :param retry_backoff_ms: The amount of time (in milliseconds) to
            back off during produce request retries. This does not equal the total time
            spent between message send attempts, since that number can be influenced
            by other kwargs, including `linger_ms` and `socket_timeout_ms`.
        :type retry_backoff_ms: int
        :param required_acks: The number of other brokers that must have
            committed the data to their log and acknowledged this to the leader
            before a request is considered complete
        :type required_acks: int
        :param ack_timeout_ms: The amount of time (in milliseconds) to wait for
            acknowledgment of a produce request on the server.
        :type ack_timeout_ms: int
        :param max_queued_messages: The maximum number of messages the producer
            can have waiting to be sent to the broker. If messages are sent
            faster than they can be delivered to the broker, the producer will
            either block or throw an exception based on the preference specified
            with block_on_queue_full.
        :type max_queued_messages: int
        :param min_queued_messages: The minimum number of messages the producer
            can have waiting in a queue before it flushes that queue to its
            broker (must be greater than 0). This paramater can be used to
            control the number of messages sent in one batch during async
            production. This parameter is automatically overridden to 1
            when `sync=True`.
        :type min_queued_messages: int
        :param linger_ms: This setting gives the upper bound on the delay for
            batching: once the producer gets min_queued_messages worth of
            messages for a broker, it will be sent immediately regardless of
            this setting.  However, if we have fewer than this many messages
            accumulated for this partition we will 'linger' for the specified
            time waiting for more records to show up. linger_ms=0 indicates no
            lingering - messages are sent as fast as possible after they are
            `produce()`d.
        :type linger_ms: int
        :param queue_empty_timeout_ms: The amount of time in milliseconds for which
            the producer's worker threads should block when no messages are available
            to flush to brokers. After each `linger_ms` interval, the worker thread
            checks for the presence of at least one message in its queue. If there is
            not at least one, it enters an "empty wait" period for
            `queue_empty_timeout_ms` before starting a new `linger_ms` wait loop. If
            `queue_empty_timeout_ms` is 0, this "empty wait" period is a noop, and
            flushes will continue to be attempted at intervals of `linger_ms`, even
            when the queue is empty. If `queue_empty_timeout_ms` is a positive integer,
            this "empty wait" period will last for at most that long, but it ends earlier
            if a message is `produce()`d before that time. If `queue_empty_timeout_ms` is
            -1, the "empty wait" period can only be stopped (and the worker thread killed)
            by a call to either `produce()` or `stop()` - it will never time out.
        :type queue_empty_timeout_ms: int
        :param block_on_queue_full: When the producer's message queue for a
            broker contains max_queued_messages, we must either stop accepting
            new messages (block) or throw an error. If True, this setting
            indicates we should block until space is available in the queue.
            If False, we should throw an error immediately.
        :type block_on_queue_full: bool
        :param max_request_size:
            The maximum size of a request in bytes. This is also effectively a
            cap on the maximum record size. Note that the server has its own
            cap on record size which may be different from this. This setting
            will limit the number of record batches the producer will send in a
            single request to avoid sending huge requests.
        :type max_request_size: int
        :param sync: Whether calls to `produce` should wait for the message to
            send before returning.  If `True`, an exception will be raised from
            `produce()` if delivery to kafka failed.
        :type sync: bool
        :param delivery_reports: If set to `True`, the producer will maintain a
            thread-local queue on which delivery reports are posted for each
            message produced.  These must regularly be retrieved through
            `get_delivery_report()`, which returns a 2-tuple of
            :class:`pykafka.protocol.Message` and either `None` (for success)
            or an `Exception` in case of failed delivery to kafka. If
            `get_delivery_report()` is not called regularly with this setting enabled,
            memory usage will grow unbounded. This setting is ignored when `sync=True`.
        :type delivery_reports: bool
        :param pending_timeout_ms: The amount of time (in milliseconds) to wait for
            delivery reports to be returned from the broker during a `produce()` call.
            Also, the time in ms to wait during a `stop()` call for all messages to be
            marked as delivered. -1 indicates that these calls should block indefinitely.
            Differs from `ack_timeout_ms` in that `ack_timeout_ms` is a value sent to the
            broker to control the broker-side timeout, while `pending_timeout_ms` is used
            internally by pykafka and not sent to the broker.
        :type pending_timeout_ms:
        :param auto_start: Whether the producer should begin communicating
            with kafka after __init__ is complete. If false, communication
            can be started with `start()`.
        :type auto_start: bool
        :param serializer: A function defining how to serialize messages to be sent
            to Kafka. A function with the signature d(value, partition_key) that
            returns a tuple of (serialized_value, serialized_partition_key). The
            arguments passed to this function are a message's value and partition key,
            and the returned data should be these fields transformed according to the
            client code's serialization logic.  See `pykafka.utils.__init__` for stock
            implemtations.
        :type serializer: function
        """
        self._cluster = cluster
        self._protocol_version = msg_protocol_version(cluster._broker_version)
        self._topic = topic
        self._partitioner = partitioner or RandomPartitioner()
        self._compression = compression
        if self._compression == CompressionType.SNAPPY and \
                platform.python_implementation == "PyPy":
            log.warning("Caution: python-snappy segfaults when attempting to compress "
                        "large messages under PyPy")
        self._max_retries = valid_int(max_retries, allow_zero=True)
        self._retry_backoff_ms = valid_int(retry_backoff_ms)
        self._required_acks = valid_int(required_acks, allow_zero=True,
                                        allow_negative=True)
        self._ack_timeout_ms = valid_int(ack_timeout_ms, allow_zero=True)
        self._max_queued_messages = valid_int(max_queued_messages, allow_zero=True)
        self._min_queued_messages = max(1, valid_int(min_queued_messages)
                                        if not sync else 1)
        self._linger_ms = valid_int(linger_ms, allow_zero=True)
        self._queue_empty_timeout_ms = valid_int(queue_empty_timeout_ms,
                                                 allow_zero=True, allow_negative=True)
        self._block_on_queue_full = block_on_queue_full
        self._max_request_size = valid_int(max_request_size)
        self._synchronous = sync
        self._worker_exception = None
        self._owned_brokers = None
        self._delivery_reports = (_DeliveryReportQueue(self._cluster.handler)
                                  if delivery_reports or self._synchronous
                                  else _DeliveryReportNone())
        self._pending_timeout_ms = pending_timeout_ms
        self._auto_start = auto_start
        self._serializer = serializer
        self._running = False
        self._update_lock = self._cluster.handler.Lock()
        if self._auto_start:
            self.start()

    def __del__(self):
        if log:  # in case log is finalized before self
            log.debug("Finalising {}".format(self))
        self.stop()

    def _raise_worker_exceptions(self):
        """Raises exceptions encountered on worker threads"""
        if self._worker_exception is not None:
            reraise(*self._worker_exception)

    def __repr__(self):
        return "<{module}.{name} at {id_}>".format(
            module=self.__class__.__module__,
            name=self.__class__.__name__,
            id_=hex(id(self))
        )

    def __enter__(self):
        """Context manager entry point - start the producer"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point - stop the producer"""
        self.stop()

    def start(self):
        """Set up data structures and start worker threads"""
        if not self._running:
            self._setup_owned_brokers()
            self._running = True
        self._raise_worker_exceptions()

    def _update(self):
        """Update the producer and cluster after an ERROR_CODE

        Also re-produces messages that were in queues at the time the update
        was triggered
        """
        # only allow one thread to be updating the producer at a time
        with self._update_lock:
            if self._owned_brokers is not None:
                for owned_broker in list(self._owned_brokers.values()):
                    owned_broker.stop()
            self._cluster.update()
            queued_messages = self._setup_owned_brokers()
            if len(queued_messages):
                log.debug("Re-producing %d queued messages after update",
                          len(queued_messages))
                for message in queued_messages:
                    self._produce(message)

    def _setup_owned_brokers(self):
        """Instantiate one OwnedBroker per broker

        If there are already OwnedBrokers instantiated, safely stop and flush them
        before creating new ones.
        """
        queued_messages = []
        if self._owned_brokers is not None:
            brokers = list(self._owned_brokers.keys())
            for broker in brokers:
                owned_broker = self._owned_brokers.pop(broker)
                owned_broker.stop()

                # loop because flush is not guaranteed to empty owned
                # broker queue
                while True:
                    batch = owned_broker.flush(self._linger_ms,
                                               self._max_request_size,
                                               release_pending=False,
                                               wait=False)
                    if not batch:
                        break
                    queued_messages.extend(batch)

        self._owned_brokers = {}
        for partition in self._topic.partitions.values():
            if partition.leader.id not in self._owned_brokers:
                self._owned_brokers[partition.leader.id] = OwnedBroker(
                    self, partition.leader)
        return queued_messages

    def stop(self):
        """Mark the producer as stopped, and wait until all messages to be sent"""
        def get_queue_readers():
            if not self._owned_brokers:
                return []
            return [owned_broker._queue_reader_worker
                    for owned_broker in self._owned_brokers.values()
                    if owned_broker.running]

        def stop_owned_brokers():
            self._wait_all()
            if self._owned_brokers is not None:
                for owned_broker in self._owned_brokers.values():
                    owned_broker.stop()

        while self._running:
            queue_readers = get_queue_readers()
            stop_owned_brokers()
            if len(queue_readers) == 0:
                self._running = False
            else:
                # The join() here works because new queue readers are spawned during the
                # execution of the old ones i.e: after a queue reader in its call to
                # producer._send_request encounters either a SocketDisconnectedError or
                # NotLeaderForPartition.ERROR_CODE it updates the cluster and sets up
                # owned_brokers, starting new queue reader threads. Because of this when
                # we call self.get_queue_readers, we get the newly created queue readers,
                # and so try to stop and join them etc ... until they all stop WITHOUT
                # encountering problems in producer._send_request.
                for queue_reader in queue_readers:
                    queue_reader.join()

    def _produce_has_timed_out(self, start_time):
        """Indicates whether enough time has passed since start_time for a `produce()`
            call to timeout
        """
        if self._pending_timeout_ms == -1:
            return False
        return time.time() * 1000 - start_time > self._pending_timeout_ms

    def produce(self, message, partition_key=None, timestamp=None):
        """Produce a message.

        :param message: The message to produce (use None to send null)
        :type message: bytes
        :param partition_key: The key to use when deciding which partition to send this
            message to. This key is passed to the `partitioner`, which may or may not
            use it in deciding the partition. The default `RandomPartitioner` does not
            use this key, but the optional `HashingPartitioner` does.
        :type partition_key: bytes
        :param timestamp: The timestamp at which the message is produced (requires
            broker_version >= 0.10.0)
        :type timestamp: `datetime.datetime`
        :return: The :class:`pykafka.protocol.Message` instance that was
            added to the internal message queue
        """
        if self._serializer is None:
            if partition_key is not None and type(partition_key) is not bytes:
                raise TypeError("Producer.produce accepts a bytes object as partition_key, "
                                "but it got '%s'", type(partition_key))
            if message is not None and type(message) is not bytes:
                raise TypeError("Producer.produce accepts a bytes object as message, but it "
                                "got '%s'", type(message))
        if timestamp is not None and self._protocol_version < 1:
            raise RuntimeError("Producer.produce got a timestamp with protocol 0")
        if not self._running:
            raise ProducerStoppedException()
        if self._serializer is not None:
            message, partition_key = self._serializer(message, partition_key)

        partitions = list(self._topic.partitions.values())
        partition_id = self._partitioner(partitions, partition_key).id

        msg = Message(value=message,
                      partition_key=partition_key,
                      partition_id=partition_id,
                      timestamp=timestamp,
                      protocol_version=self._protocol_version,
                      # We must pass our thread-local Queue instance directly,
                      # as results will be written to it in a worker thread
                      delivery_report_q=self._delivery_reports.queue)
        self._produce(msg)

        if self._synchronous:
            req_time = time.time() * 1000
            reported_msg = None
            while not self._produce_has_timed_out(req_time):
                self._raise_worker_exceptions()
                self._cluster.handler.sleep()
                try:
                    reported_msg, exc = self.get_delivery_report(timeout=1)
                    break
                except Empty:
                    continue
                except ValueError:
                    raise ProduceFailureError("Error retrieving delivery report")
            if reported_msg is not msg:
                raise ProduceFailureError("Delivery report not received after timeout")
            if exc is not None:
                raise exc
        self._raise_worker_exceptions()
        return msg

    def get_delivery_report(self, block=True, timeout=None):
        """Fetch delivery reports for messages produced on the current thread

        Returns 2-tuples of a `pykafka.protocol.Message` and either `None`
        (for successful deliveries) or `Exception` (for failed deliveries).
        This interface is only available if you enabled `delivery_reports` on
        init (and you did not use `sync=True`)

        :param block: Whether to block on dequeueing a delivery report
        :type block: bool
        :param timeout: How long (in seconds) to block before returning None
        ;type timeout: int
        """
        try:
            return self._delivery_reports.queue.get(block, timeout)
        except AttributeError:
            raise KafkaException("Delivery-reporting is disabled")

    def _produce(self, message):
        """Enqueue a message for the relevant broker
        Attempts to update metadata in response to missing brokers.
        :param message: Message with valid `partition_id`, ready to be sent
        :type message: `pykafka.protocol.Message`
        """
        success = False
        retry = 0
        while not success:
            leader_id = self._topic.partitions[message.partition_id].leader.id
            if leader_id in self._owned_brokers:
                self._owned_brokers[leader_id].enqueue(message)
                success = True
            else:
                retry += 1
                if retry < 10:
                    log.debug("Failed to enqueue produced message. Updating metdata.")
                    self._update()
                else:
                    raise ProduceFailureError("Message could not be enqueued due to missing broker "
                                              "metadata for broker {}".format(leader_id))
                success = False

    def _mark_as_delivered(self, owned_broker, message_batch, req):
        owned_broker.increment_messages_pending(-1 * len(message_batch))
        req.delivered += len(message_batch)
        for msg in message_batch:
            self._delivery_reports.put(msg)

    def _send_request(self, message_batch, owned_broker):
        """Send the produce request to the broker and handle the response.

        :param message_batch: An iterable of messages to send
        :type message_batch: iterable of `pykafka.protocol.Message`
        :param owned_broker: The broker to which to send the request
        :type owned_broker: :class:`pykafka.producer.OwnedBroker`
        """
        req = ProduceRequest(
            compression_type=self._compression,
            required_acks=self._required_acks,
            timeout=self._ack_timeout_ms,
            broker_version=self._cluster._broker_version
        )
        req.delivered = 0
        for msg in message_batch:
            req.add_message(msg, self._topic.name, msg.partition_id)
        log.debug("Sending %d messages to broker %d",
                  len(message_batch), owned_broker.broker.id)

        def _get_partition_msgs(partition_id, req):
            """Get all the messages for the partitions from the request."""
            return (
                mset
                for topic, partitions in iteritems(req.msets)
                for p_id, mset in iteritems(partitions)
                if p_id == partition_id
            )

        try:
            response = owned_broker.broker.produce_messages(req)
            if self._required_acks == 0:  # and thus, `response` is None
                self._mark_as_delivered(owned_broker, message_batch, req)
                return

            # Kafka either atomically appends or rejects whole MessageSets, so
            # we define a list of potential retries thus:
            to_retry = []  # (MessageSet, Exception) tuples

            for topic, partitions in iteritems(response.topics):
                for partition, presponse in iteritems(partitions):
                    if presponse.err == 0:
                        messages = req.msets[topic][partition].messages
                        for i, message in enumerate(messages):
                            message.offset = presponse.offset + i
                        self._mark_as_delivered(owned_broker, messages, req)
                        continue  # All's well
                    if presponse.err == NotLeaderForPartition.ERROR_CODE:
                        # Update cluster metadata to get new leader
                        self._update()
                    info = "Produce request for {}/{} to {}:{} failed with error code {}.".format(
                        topic,
                        partition,
                        owned_broker.broker.host,
                        owned_broker.broker.port,
                        presponse.err)
                    log.warning(info)
                    exc = ERROR_CODES[presponse.err](info)
                    to_retry.extend(
                        (mset, exc)
                        for mset in _get_partition_msgs(partition, req))
        except (SocketDisconnectedError, struct.error) as exc:
            log.warning('Error encountered when producing to broker %s:%s. Retrying.',
                        owned_broker.broker.host,
                        owned_broker.broker.port)
            self._update()
            to_retry = [
                (mset, exc)
                for topic, partitions in iteritems(req.msets)
                for p_id, mset in iteritems(partitions)
            ]

        log.debug("Successfully sent {}/{} messages to broker {}".format(
            req.delivered, len(message_batch), owned_broker.broker.id))

        if to_retry:
            self._cluster.handler.sleep(self._retry_backoff_ms / 1000)
            owned_broker.increment_messages_pending(-1 * len(to_retry))
            for mset, exc in to_retry:
                # XXX arguably, we should try to check these non_recoverables
                # for individual messages in _produce and raise errors there
                # right away, rather than failing a whole batch here?
                non_recoverable = type(exc) in (InvalidMessageSize,
                                                MessageSizeTooLarge)
                for msg in mset.messages:
                    if (non_recoverable or msg.produce_attempt >= self._max_retries):
                        self._delivery_reports.put(msg, exc)
                        log.error("Message not delivered!! %r" % exc)
                    else:
                        msg.produce_attempt += 1
                        self._produce(msg)

    def _wait_all(self):
        """Block until all pending messages are sent or until pending_timeout_ms

        "Pending" messages are those that have been used in calls to `produce`
        and have not yet been acknowledged in a response from the broker
        """
        log.info("Blocking until all messages are sent or until pending_timeout_ms")
        start_time = time.time() * 1000
        while any(q.message_is_pending() for q in itervalues(self._owned_brokers)) and \
                not self._produce_has_timed_out(start_time):
            self._cluster.handler.sleep(.3)
            self._raise_worker_exceptions()


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
    :ivar messages_pending: A counter indicating how many messages have been
        enqueued for this broker and not yet sent in a request.
    :type messages_pending: int
    :ivar producer: The producer to which this OwnedBroker instance belongs
    :type producer: :class:`pykafka.producer.AsyncProducer`
    :param auto_start: Whether the OwnedBroker should start flushing all
        waiting messages and send to kafka after __init__ is complete. If
        false, communication can be started with `start()`.
    :type auto_start: bool
    """
    def __init__(self, producer, broker, auto_start=True):
        self.producer = weakref.proxy(producer)
        self.broker = broker
        self.lock = self.producer._cluster.handler.RLock()
        self.flush_ready = self.producer._cluster.handler.Event()
        self.has_message = self.producer._cluster.handler.Event()
        self.slot_available = self.producer._cluster.handler.Event()
        self.queue = deque()
        self.messages_pending = 0
        self.running = True
        self._auto_start = auto_start

        if self._auto_start:
            self.start()

    def cleanup(self):
        if not self.slot_available.is_set():
            self.slot_available.set()

    def start(self):
        def queue_reader():
            while self.running:
                try:
                    batch = self.flush(self.producer._linger_ms, self.producer._max_request_size)
                    if batch:
                        self.producer._send_request(batch, self)
                except Exception:
                    # surface all exceptions to the main thread
                    self.producer._worker_exception = sys.exc_info()
                    break
            self.cleanup()
            log.info("Worker exited for broker %s:%s", self.broker.host,
                     self.broker.port)
        log.info("Starting new produce worker for broker %s", self.broker.id)
        name = "pykafka.OwnedBroker.queue_reader for broker {}".format(self.broker.id)
        self._queue_reader_worker = self.producer._cluster.handler.spawn(
            queue_reader, name=name)

    def stop(self):
        # explicitly set has_message to kill any infinite waits triggered by
        # queue_empty_timeout_ms=-1
        with self.lock:
            if not self.has_message.is_set():
                self.has_message.set()
        self.running = False

    def increment_messages_pending(self, amnt):
        with self.lock:
            self.messages_pending += amnt
            self.messages_pending = max(0, self.messages_pending)

    def message_is_pending(self):
        """
        Indicates whether there are currently any messages that have been
            `produce()`d and not yet sent to the broker
        """
        return self.messages_pending > 0

    def enqueue(self, message):
        """Push message onto the queue

        :param message: The message to push onto the queue
        :type message: `pykafka.protocol.Message`
        """
        self._wait_for_slot_available()
        with self.lock:
            self.queue.appendleft(message)
            self.increment_messages_pending(1)
            if len(self.queue) >= self.producer._min_queued_messages:
                if not self.flush_ready.is_set():
                    self.flush_ready.set()
            if not self.has_message.is_set():
                self.has_message.set()

    def flush(self, linger_ms, max_request_size, release_pending=False, wait=True):
        """Pop messages from the end of the queue

        :param linger_ms: How long (in milliseconds) to wait for the queue
            to contain messages before flushing
        :type linger_ms: int
        :param max_request_size: The max size should each batch of messages
            should be in bytes
        :type max_request_size: int
        :param release_pending: Whether to decrement the messages_pending
            counter when the queue is flushed. True means that the messages
            popped from the queue will be discarded unless re-enqueued
            by the caller.
        :type release_pending: bool
        :param wait: If True, wait for the event indicating a flush is ready. If False,
            attempt a flush immediately without waiting
        :type wait: bool
        """
        # Q: why not simply wait for flush_ready here? do we need a separate Event for
        #    has_message?
        # A: If we're blocking on flush_ready with an empty queue, a single event arriving
        #    does not mean we're ready to flush. We could flush whenever the
        #    current linger_ms interval ends, but the better way is to pause the linger
        #    loop when the queue is empty, restarting it when a message is added. Doing
        #    this without two Events would require _wait_for_flush_ready to be modal,
        #    returning a value indicating whether a flush should happen or whether it
        #    returned for the sole purpose of unblocking the linger loop. This design is
        #    cleaner.
        self._wait_for_has_message(self.producer._queue_empty_timeout_ms)
        if wait:
            self._wait_for_flush_ready(linger_ms)
        with self.lock:
            batch = []
            batch_size_in_bytes = 0
            while len(self.queue) > 0:
                if not self.running:
                    return []
                peeked_message = self.queue[-1]

                if peeked_message and peeked_message.value is not None:
                    if len(peeked_message) > max_request_size:
                        exc = MessageSizeTooLarge(
                            "Message size larger than max_request_size: {}".format(max_request_size)
                        )
                        log.warning(exc)
                        # bind the MessageSizeTooLarge error the delivery
                        # report and remove it from the producer queue
                        message = self.queue.pop()
                        # don't use producer.delivery_report_q here to enable
                        # integration tests that test the OwnedBroker without a
                        # Producer
                        if peeked_message.delivery_report_q is not None:
                            peeked_message.delivery_report_q.put((message, exc))
                        # remove from pending message count
                        self.increment_messages_pending(-1)
                        continue

                    # test if adding the message would go over the
                    # max_request_size. if it would, break out of loop
                    elif batch_size_in_bytes + len(peeked_message) > max_request_size:
                        log.debug("max_request_size reached. producing batch")
                        # if we did not fully empty the queue. reset the
                        # flush_ready so we send another batch immediately
                        self.flush_ready.set()
                        break

                message = self.queue.pop()
                batch_size_in_bytes += len(message)
                batch.append(message)

            if release_pending:
                self.increment_messages_pending(-1 * len(batch))
            if not self.slot_available.is_set():
                self.slot_available.set()
        if not self.running:
            return []
        return batch

    def _wait_for_flush_ready(self, linger_ms):
        """Block until the queue is ready to be flushed

        If the queue does not contain at least one message after blocking for
        `linger_ms` milliseconds, return.

        :param linger_ms: How long (in milliseconds) to wait for the queue
            to contain messages before returning
        :type linger_ms: int
        """
        if len(self.queue) < self.producer._min_queued_messages:
            with self.lock:
                if len(self.queue) < self.producer._min_queued_messages:
                    self.flush_ready.clear()
            if linger_ms > 0:
                self.flush_ready.wait((linger_ms / 1000))

    def _wait_for_has_message(self, timeout_ms):
        """Block until the queue has at least one slot containing a message

        :param timeout_ms: The amount of time in milliseconds to wait for a message
            to be enqueued. -1 indicates infinite waiting; in this case a thread waiting
            on this call can only be killed by a call to `stop()`.
        :type timeout_ms: int
        """
        if len(self.queue) == 0 and self.running:
            with self.lock:
                if len(self.queue) == 0 and self.running:
                    self.has_message.clear()
            if timeout_ms != -1:
                self.has_message.wait(timeout_ms / 1000)
            else:
                # infinite wait that doesn't break under gevent
                while not self.has_message.is_set():
                    self.producer._cluster.handler.sleep()
                    self.has_message.wait(5)

    def _wait_for_slot_available(self):
        """Block until the queue has at least one slot not containing a message"""
        if len(self.queue) >= self.producer._max_queued_messages:
            with self.lock:
                if len(self.queue) >= self.producer._max_queued_messages:
                    self.slot_available.clear()
            if self.producer._block_on_queue_full:
                while not self.slot_available.is_set():
                    self.producer._cluster.handler.sleep()
                    self.slot_available.wait(5)
            else:
                raise ProducerQueueFullError("Queue full for broker %d",
                                             self.broker.id)


class _DeliveryReportQueue(threading.local):
    """Helper that instantiates a new report queue on every calling thread"""
    def __init__(self, handler):
        self.queue = handler.Queue()

    @staticmethod
    def put(msg, exc=None):
        msg.delivery_report_q.put((msg, exc))


class _DeliveryReportNone(object):
    """Stand-in for when _DeliveryReportQueue has been disabled"""
    def __init__(self):
        self.queue = None

    @staticmethod
    def put(msg, exc=None):
        return
