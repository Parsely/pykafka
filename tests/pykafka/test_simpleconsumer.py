import mock
import os
import time
import unittest2

from pykafka import KafkaClient
from pykafka.simpleconsumer import OwnedPartition
from pykafka.test.kafka_instance import KafkaInstance, KafkaConnection
from pykafka.test.utils import get_cluster, stop_cluster


class TestSimpleConsumer(unittest2.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.topic_name = 'test-data'
        cls.kafka.create_topic(cls.topic_name, 3, 2)
        cls.kafka.produce_messages(
            cls.topic_name,
            ('msg {}'.format(i) for i in xrange(1000))
        )
        cls.client = KafkaClient(cls.kafka.brokers)

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def test_consume(self):
        consumer = self.client.topics[self.topic_name].get_simple_consumer()
        try:
            messages = [consumer.consume() for _ in xrange(1000)]
            self.assertEquals(len(messages), 1000)
        finally:
            consumer.stop()

    def test_offset_commit(self):
        consumer = self.client.topics[self.topic_name].get_simple_consumer('test_offset_commit')
        try:
            [consumer.consume() for _ in xrange(100)]
            consumer.commit_offsets()
            res = consumer.fetch_offsets()
            offset_sum = sum(r[1].offset for r in res if r[1].offset >= 0)
            self.assertEquals(offset_sum, 99)  # 0-indexed
        finally:
            consumer.stop()


    def test_offset_resume(self):
        consumer = self.client.topics[self.topic_name].get_simple_consumer('test_offset_resume')
        try:
            [consumer.consume() for _ in xrange(100)]
            consumer.commit_offsets()
        finally:
            consumer.stop()

        consumer = self.client.topics[self.topic_name].get_simple_consumer('test_offset_resume')
        try:
            res = consumer.fetch_offsets()
            offset_sum = sum(p.last_offset_consumed
                             for p in consumer.partitions
                             if p.last_offset_consumed >= 0)
            self.assertEquals(offset_sum, 99)  # 0-indexed
        finally:
            consumer.stop()


class TestOwnedPartition(unittest2.TestCase):
    def test_partition_saves_offset(self):
        msgval = "test"
        op = OwnedPartition(None)

        message = mock.Mock()
        message.value = msgval
        message.offset = 20

        op.enqueue_messages([message])
        self.assertEqual(op.message_count, 1)
        ret_message = op.consume()
        self.assertEqual(op.last_offset_consumed, message.offset)
        self.assertEqual(op.next_offset, message.offset + 1)
        self.assertNotEqual(ret_message, None)
        self.assertEqual(ret_message.value, msgval)

    def test_partition_rejects_old_message(self):
        last_offset = 400
        op = OwnedPartition(None)
        op.last_offset_consumed = last_offset

        message = mock.Mock()
        message.value = "test"
        message.offset = 20

        op.enqueue_messages([message])
        self.assertEqual(op.message_count, 0)
        op.consume()
        self.assertEqual(op.last_offset_consumed, last_offset)

    def test_partition_consume_empty_queue(self):
        op = OwnedPartition(None)

        message = op.consume()
        self.assertEqual(message, None)

    def test_partition_offset_commit_request(self):
        topic = mock.Mock()
        topic.name = "test_topic"
        partition = mock.Mock()
        partition.topic = topic
        partition.id = 12345

        op = OwnedPartition(partition)
        op.last_offset_consumed = 200

        rqtime = int(time.time())
        request = op.build_offset_commit_request()

        self.assertEqual(request.topic_name, topic.name)
        self.assertEqual(request.partition_id, partition.id)
        self.assertEqual(request.offset, op.last_offset_consumed)
        self.assertEqual(request.metadata, 'pykafka')

    def test_partition_offset_fetch_request(self):
        topic = mock.Mock()
        topic.name = "test_topic"
        partition = mock.Mock()
        partition.topic = topic
        partition.id = 12345

        op = OwnedPartition(partition)

        request = op.build_offset_fetch_request()

        self.assertEqual(request.topic_name, topic.name)
        self.assertEqual(request.partition_id, partition.id)

    def test_partition_offset_counters(self):
        res = mock.Mock()
        res.offset = 400

        op = OwnedPartition(None)
        op.set_offset(res.offset)

        self.assertEqual(op.last_offset_consumed, res.offset)
        self.assertEqual(op.next_offset, res.offset + 1)


if __name__ == "__main__":
    unittest2.main()
