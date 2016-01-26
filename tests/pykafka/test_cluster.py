import unittest
from uuid import uuid4

from pykafka import KafkaClient, Topic
from pykafka.utils.compat import itervalues
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

    def test_exclude_internal_topics(self):
        """Test exclude_internal_topics setting

        See also #277 for a related bug.
        """
        topic_name = b"__starts_with_underscores"
        with self.assertRaises(KeyError):
            topic = self.client.topics[topic_name]

        client = KafkaClient(self.kafka.brokers, exclude_internal_topics=False)
        topic = client.topics[topic_name]
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

    def test_zk_connect(self):
        """Clusters started with broker lists and zk connect strings should get same brokers"""
        zk_client = KafkaClient(zookeeper_hosts=self.kafka.zookeeper)
        kafka_client = KafkaClient(hosts=self.kafka.brokers)
        zk_brokers = ["{}:{}".format(b.host, b.port)
                      for b in itervalues(zk_client.brokers)]
        kafka_brokers = ["{}:{}".format(b.host, b.port)
                         for b in itervalues(kafka_client.brokers)]
        self.assertEqual(zk_brokers, kafka_brokers)

if __name__ == "__main__":
    unittest.main()
