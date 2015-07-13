from contextlib import contextmanager
import mock
import unittest2

from pykafka import KafkaClient
from pykafka.simpleconsumer import OwnedPartition
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
            ('msg {i}'.format(i=i) for i in xrange(1000))
        )
        cls.client = KafkaClient(cls.kafka.brokers)

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    @contextmanager
    def _get_simple_consumer(self, **kwargs):
        # Mostly spun out so we can override it in TestRdKafkaSimpleConsumer
        topic = self.client.topics[self.topic_name]
        consumer = topic.get_simple_consumer(**kwargs)
        yield consumer
        consumer.stop()

    def test_consume(self):
        with self._get_simple_consumer() as consumer:
            messages = [consumer.consume() for _ in xrange(1000)]
            self.assertEquals(len(messages), 1000)

    def test_offset_commit(self):
        """Check fetched offsets match pre-commit internal state"""
        with self._get_simple_consumer(
                consumer_group='test_offset_commit') as consumer:
            [consumer.consume() for _ in xrange(100)]
            offsets_committed = self._currently_held_offsets(consumer)
            consumer.commit_offsets()

            offsets_fetched = dict((r[0], r[1].offset)
                                   for r in consumer.fetch_offsets())
            self.assertEquals(offsets_fetched, offsets_committed)

    def test_offset_resume(self):
        """Check resumed internal state matches committed offsets"""
        with self._get_simple_consumer(
                consumer_group='test_offset_resume') as consumer:
            [consumer.consume() for _ in xrange(100)]
            offsets_committed = self._currently_held_offsets(consumer)
            consumer.commit_offsets()

        with self._get_simple_consumer(
                consumer_group='test_offset_resume') as consumer:
            offsets_resumed = self._currently_held_offsets(consumer)
            self.assertEquals(offsets_resumed, offsets_committed)

    @staticmethod
    def _currently_held_offsets(consumer):
        return dict((p.partition.id, p.last_offset_consumed)
                    for p in consumer._partitions.itervalues())


class TestOwnedPartition(unittest2.TestCase):
    def test_partition_saves_offset(self):
        msgval = "test"
        partition = mock.MagicMock()
        op = OwnedPartition(partition)

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
