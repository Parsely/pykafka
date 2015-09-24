import unittest
from uuid import uuid4

from pykafka import KafkaClient, Topic
from pykafka.test.utils import get_cluster, stop_cluster


class ClusterIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.client = KafkaClient(cls.kafka.brokers)

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def test_topic_autocreate(self):
        topic_name = uuid4().hex.encode()
        topic = self.client.topics[topic_name]
        self.assertTrue(isinstance(topic, Topic))

    def test_topic_updates(self):
        startlen = len(self.client.topics)
        name_a = uuid4().hex.encode()
        name_b = uuid4().hex.encode()

        self.client.topics[name_a]
        self.client.topics[name_a]  # 2nd time shouldn't affect len(topics)
        self.client.topics[name_b]
        self.assertEqual(len(self.client.topics), startlen + 2)
        self.assertIn(name_a, self.client.topics)

        self.kafka.delete_topic(name_a)
        self.client.update_cluster()
        self.assertEqual(len(self.client.topics), startlen + 1)
        self.assertNotIn(name_a, self.client.topics)
        self.assertIn(name_b, self.client.topics)

        self.kafka.delete_topic(name_b)
        self.client.update_cluster()
        self.assertEqual(len(self.client.topics), startlen)
        self.assertNotIn(name_b, self.client.topics)


if __name__ == "__main__":
    unittest.main()
