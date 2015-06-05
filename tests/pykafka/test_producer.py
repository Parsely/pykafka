import unittest2

from pykafka import KafkaClient
from pykafka.test.utils import get_cluster, stop_cluster


class ProducerIntegrationTests(unittest2.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.topic_name = 'test-data'
        cls.kafka.create_topic(cls.topic_name, 3, 2)
        cls.client = KafkaClient(cls.kafka.brokers)

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def test_produce(self):
        try:
            prod = self.client.topics[self.topic_name].get_producer(batch_size=5)
            prod.produce(['msg test'])

            consumer = self.client.topics[self.topic_name].get_balanced_consumer('test_consume', zookeeper_connect=self.kafka.zookeeper)
            messages = [consumer.consume() for i in xrange(1)]
            self.assertTrue(len(messages) == 1)
        finally:
            consumer.stop()

if __name__ == "__main__":
    unittest2.main()
