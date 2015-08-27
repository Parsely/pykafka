from pykafka.test.utils import unittest
from pykafka import KafkaClient
from pykafka.test.utils import get_cluster, stop_cluster


class TestPartitionInfo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.topic_name = 'test-data'
        cls.kafka.create_topic(cls.topic_name, 3, 2)
        cls.client = KafkaClient(cls.kafka.brokers)
        topic = cls.client.topics[cls.topic_name]
        cls.producer = topic.get_producer(min_queued_messages=1)
        cls.total_messages = 99
        for i in range(cls.total_messages):
            cls.producer.produce("message %s" % i)

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def test_can_get_earliest_offset(self):
        partitions = self.client.topics[self.topic_name].partitions
        for partition in partitions.values():
            self.assertEqual(0, partition.earliest_available_offset())

    def test_can_get_latest_offset(self):
        partitions = self.client.topics[self.topic_name].partitions
        for partition in partitions.values():
            self.assertTrue(partition.latest_available_offset())

if __name__ == "__main__":
    unittest.main()
