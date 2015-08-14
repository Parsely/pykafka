import unittest2
from uuid import uuid4

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
        cls.consumer = cls.client.topics[cls.topic_name].get_simple_consumer(
            consumer_timeout_ms=1000)

    @classmethod
    def tearDownClass(cls):
        cls.consumer.stop()
        stop_cluster(cls.kafka)

    def test_produce(self):
        # unique bytes, just to be absolutely sure we're not fetching data
        # produced in a previous test
        payload = uuid4().bytes

        prod = self.client.topics[self.topic_name].get_sync_producer()
        prod.produce([payload])

        # set a timeout so we don't wait forever if we break producer code
        message = self.consumer.consume()
        self.assertTrue(message.value == payload)

    def test_async_produce(self):
        payload = uuid4().bytes

        prod = self.client.topics[self.topic_name].get_producer()
        prod.produce([payload])

        message = self.consumer.consume()
        self.assertTrue(message.value == payload)

    def test_async_produce_context(self):
        payload = uuid4().bytes

        with self.client.topics[self.topic_name].get_producer() as producer:
            producer.produce([payload])

        message = self.consumer.consume()
        self.assertTrue(message.value == payload)


if __name__ == "__main__":
    unittest2.main()
