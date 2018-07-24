from __future__ import division

import mock
import os
import platform
import pytest
import random
import time
import types
import unittest2
from uuid import uuid4

try:
    import gevent
except ImportError:
    gevent = None

try:
    from pykafka.rdkafka import _rd_kafka  # noqa
    RDKAFKA = True
except ImportError:
    RDKAFKA = False  # C extension not built

from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import (MessageSizeTooLarge, ProducerQueueFullError,
                                ProduceFailureError)
from pykafka.partitioners import hashing_partitioner
from pykafka.protocol import Message
from pykafka.test.utils import get_cluster, stop_cluster, retry
from pykafka.common import CompressionType
from pykafka.producer import OwnedBroker
from pykafka.utils import serialize_utf8, deserialize_utf8

kafka_version = os.environ.get('KAFKA_VERSION', '0.8.0')


class ProducerIntegrationTests(unittest2.TestCase):
    maxDiff = None
    USE_RDKAFKA = False
    USE_GEVENT = False

    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.topic_name = b'test-data'
        cls.kafka.create_topic(cls.topic_name, 3, 2)
        cls.client = KafkaClient(cls.kafka.brokers,
                                 use_greenlets=cls.USE_GEVENT,
                                 broker_version=kafka_version)

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def _get_producer(self, **kwargs):
        topic = self.client.topics[self.topic_name]
        return topic.get_producer(use_rdkafka=self.USE_RDKAFKA, **kwargs)

    def _get_consumer(self, **kwargs):
        return self.client.topics[self.topic_name].get_simple_consumer(
            consumer_timeout_ms=1000,
            auto_offset_reset=OffsetType.LATEST,
            reset_offset_on_start=True,
            **kwargs
        )

    def test_produce(self):
        # unique bytes, just to be absolutely sure we're not fetching data
        # produced in a previous test
        payload = uuid4().bytes

        consumer = self._get_consumer()

        prod = self._get_producer(sync=True, min_queued_messages=1)
        prod.produce(payload)

        message = consumer.consume()
        assert message.value == payload

    def test_produce_utf8(self):
        payload = u"{}".format(random.random())
        consumer = self._get_consumer(deserializer=deserialize_utf8)
        prod = self._get_producer(sync=True, min_queued_messages=1,
                                  serializer=serialize_utf8)
        prod.produce(payload)
        message = consumer.consume()
        assert message.value == payload

    def test_sync_produce_raises(self):
        """Ensure response errors are raised in produce() if sync=True"""
        with self._get_producer(sync=True, min_queued_messages=1) as prod:
            with self.assertRaises(MessageSizeTooLarge):
                prod.produce(10 ** 7 * b" ")

        if not self.USE_RDKAFKA:
            # ensure that a crash on a worker thread still raises exception in sync mode
            p = self._get_producer(sync=True)

            def stub_send_request(self, message_batch, owned_broker):
                1 / 0
            p._send_request = types.MethodType(stub_send_request, p)
            with self.assertRaises(ZeroDivisionError):
                p.produce(b"test")

    def test_sync_produce_doesnt_hang(self):
        producer = self._get_producer(sync=True)

        def stub_mark(w, x, y, z):
            return None
        # simulate delivery report being lost
        producer._mark_as_delivered = types.MethodType(stub_mark, producer)
        producer._delivery_reports = mock.MagicMock()
        with self.assertRaises(ProduceFailureError):
            producer.produce(b"test")

    def test_produce_hashing_partitioner(self):
        # unique bytes, just to be absolutely sure we're not fetching data
        # produced in a previous test
        payload = uuid4().bytes

        consumer = self._get_consumer()

        prod = self._get_producer(
            sync=True,
            min_queued_messages=1,
            partitioner=hashing_partitioner)
        prod.produce(payload, partition_key=b"dummy")

        # set a timeout so we don't wait forever if we break producer code
        message = consumer.consume()
        assert message.value == payload

    def test_async_produce(self):
        payload = uuid4().bytes

        consumer = self._get_consumer()

        prod = self._get_producer(min_queued_messages=1, delivery_reports=True)
        prod.produce(payload)

        report = prod.get_delivery_report()
        self.assertEqual(report[0].value, payload)
        self.assertIsNone(report[1])
        self.assertGreaterEqual(report[0].offset, 0)

        message = consumer.consume()
        assert message.value == payload

    def test_recover_disconnected(self):
        """Test our retry-loop with a recoverable error"""
        payload = uuid4().bytes
        prod = self._get_producer(min_queued_messages=1, delivery_reports=True)

        for broker in self.client.brokers.values():
            broker._connection.disconnect()

        prod.produce(payload)
        report = prod.get_delivery_report()
        self.assertIsNone(report[1])

    def test_async_produce_context(self):
        """Ensure that the producer works as a context manager"""
        payload = uuid4().bytes

        consumer = self._get_consumer()
        with self._get_producer(min_queued_messages=1) as producer:
            producer.produce(payload)

        message = consumer.consume()
        assert message.value == payload

    def test_async_produce_queue_full(self):
        """Ensure that the producer raises an error when its queue is full"""
        consumer = self._get_consumer()
        with self._get_producer(block_on_queue_full=False,
                                max_queued_messages=1,
                                linger_ms=1000) as producer:
            with self.assertRaises(ProducerQueueFullError):
                while True:
                    producer.produce(uuid4().bytes)
        while consumer.consume() is not None:
            time.sleep(.05)

    def test_async_produce_lingers(self):
        """Ensure that the context manager waits for linger_ms milliseconds"""
        if self.USE_RDKAFKA:
            pytest.skip("rdkafka uses different lingering mechanism")
        linger = 3
        consumer = self._get_consumer()
        with self._get_producer(linger_ms=linger * 1000) as producer:
            start = time.time()
            producer.produce(uuid4().bytes)
            producer.produce(uuid4().bytes)
        self.assertTrue(int(time.time() - start) >= int(linger))
        consumer.consume()
        consumer.consume()

    def test_async_produce_thread_exception(self):
        """Ensure that an exception on a worker thread is raised to the main thread"""
        consumer = self._get_consumer()
        with self.assertRaises(AttributeError):
            with self._get_producer(min_queued_messages=1) as producer:
                # get some dummy data into the queue that will cause a crash
                # when flushed:
                msg = Message("stuff", partition_id=0)
                del msg.value
                producer._produce(msg)
        while consumer.consume() is not None:
            time.sleep(.05)

    def test_required_acks(self):
        """Test with non-default values for `required_acks`

        See #278 for a related bug.  Here, we only test that no exceptions
        occur (hence `sync=True`, which would surface most exceptions)
        """
        kwargs = dict(linger_ms=1, sync=True, required_acks=0)
        prod = self._get_producer(**kwargs)
        prod.produce(uuid4().bytes)

        kwargs["required_acks"] = -1
        prod = self._get_producer(**kwargs)
        prod.produce(uuid4().bytes)

    def test_null_payloads(self):
        """Test that None is accepted as a null payload"""
        consumer = self._get_consumer()
        prod = self._get_producer(sync=True, min_queued_messages=1)
        prod.produce(None)
        self.assertIsNone(consumer.consume().value)
        prod.produce(None, partition_key=b"whatever")
        self.assertIsNone(consumer.consume().value)
        prod.produce(b"")  # empty string should be distinguished from None
        self.assertEqual(b"", consumer.consume().value)

    def test_owned_broker_flush_message_larger_then_max_request_size(self):
        """Test that producer batches messages into the batches no larger than
        `max_request_size`
        """
        large_payload = b''.join([uuid4().bytes for i in range(50000)])

        producer = self._get_producer(
            auto_start=False
        )
        # setup producer but do not start actually sending messages
        partition = producer._topic.partitions[0]
        owned_broker = OwnedBroker(producer, partition.leader, auto_start=False)

        delivery_report_queue = producer._cluster.handler.Queue()
        msg = Message(
            large_payload,
            partition_id=0,
            delivery_report_q=delivery_report_queue
        )

        owned_broker.enqueue(msg)

        max_request_size = 1000
        assert max_request_size < len(msg)
        owned_broker.flush(0, max_request_size)
        q_msg, exc = delivery_report_queue.get()
        assert q_msg is msg
        assert isinstance(exc, MessageSizeTooLarge)

    def test_owned_broker_flush_batching_by_max_request_size(self):
        """Test that producer batches messages into the batches no larger than
        `max_request_size`
        """
        large_payload = b''.join([uuid4().bytes for i in range(5000)])

        producer = self._get_producer(
            auto_start=False
        )
        # setup producer but do not start actually sending messages
        partition = producer._topic.partitions[0]
        owned_broker = OwnedBroker(producer, partition.leader, auto_start=False)

        for i in range(100):
            msg = Message(large_payload, partition_id=0)
            owned_broker.enqueue(msg)

        batch = owned_broker.flush(0, producer._max_request_size)
        assert len(batch) < 100
        assert sum([len(m.value) for m in batch]) < producer._max_request_size

        # iterate through the rest of the batches and test the same invariant
        while batch:
            batch = owned_broker.flush(0, producer._max_request_size)
            assert len(batch) < 100
            assert sum([len(m.value) for m in batch]) < producer._max_request_size

    def test_async_produce_compression_large_message(self):
        # TODO: make payload size bigger once pypy snappy compression issue is
        # fixed
        large_payload = b''.join([uuid4().bytes for i in range(5)])
        consumer = self._get_consumer()

        prod = self._get_producer(
            compression=CompressionType.SNAPPY,
            delivery_reports=True
        )
        prod.produce(large_payload)

        report = prod.get_delivery_report()
        self.assertEqual(report[0].value, large_payload)
        self.assertIsNone(report[1])

        message = consumer.consume()
        assert message.value == large_payload

        for i in range(10):
            prod.produce(large_payload)

        # use retry logic to loop over delivery reports and ensure we can
        # produce a group of large messages
        reports = []

        def ensure_all_messages_produced():
            report = prod.get_delivery_report()
            reports.append(report)
            assert len(reports) == 10
        retry(ensure_all_messages_produced, retry_time=30, wait_between_tries=0.5)

        for report in reports:
            self.assertEqual(report[0].value, large_payload)
            self.assertIsNone(report[1])

        # cleanup and consumer all messages
        msgs = []

        def ensure_all_messages_consumed():
            msg = consumer.consume()
            if msg:
                msgs.append(msg)
            assert len(msgs) == 10
        retry(ensure_all_messages_consumed, retry_time=15)

    def test_async_produce_large_message(self):

        consumer = self._get_consumer()
        large_payload = b''.join([uuid4().bytes for i in range(50000)])
        assert len(large_payload) / 1024 / 1024 < 1.0

        prod = self._get_producer(delivery_reports=True, linger_ms=1000)
        prod.produce(large_payload)

        report = prod.get_delivery_report()
        self.assertEqual(report[0].value, large_payload)
        self.assertIsNone(report[1])

        message = consumer.consume()
        assert message.value == large_payload

        for i in range(10):
            prod.produce(large_payload)

        # use retry logic to loop over delivery reports and ensure we can
        # produce a group of large messages
        reports = []

        def ensure_all_messages_produced():
            report = prod.get_delivery_report()
            reports.append(report)
            assert len(reports) == 10
        retry(ensure_all_messages_produced, retry_time=30, wait_between_tries=0.5)

        for report in reports:
            self.assertEqual(report[0].value, large_payload)
            self.assertIsNone(report[1])

        # cleanup and consumer all messages
        msgs = []

        def ensure_all_messages_consumed():
            msg = consumer.consume()
            if msg:
                msgs.append(msg)
            assert len(msgs) == 10
        retry(ensure_all_messages_consumed, retry_time=15)


@pytest.mark.skipif(not RDKAFKA, reason="rdkafka")
class TestRdKafkaProducer(ProducerIntegrationTests):
    USE_RDKAFKA = True


@pytest.mark.skipif(platform.python_implementation() == "PyPy" or gevent is None,
                    reason="Unresolved crashes")
class TestGEventProducer(ProducerIntegrationTests):
    USE_GEVENT = True


if __name__ == "__main__":
    unittest2.main()
