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

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def test_produce(self):
        try:
            # unique bytes, just to be absolutely sure we're not fetching data
            # produced in a previous test
            payload = uuid4().bytes

            prod = self.client.topics[self.topic_name].get_producer(batch_size=5)
            prod.produce([payload])

            # set a timeout so we don't wait forever if we break producer code
            consumer = self.client.topics[self.topic_name].get_simple_consumer(
                consumer_timeout_ms=1000)
            message = consumer.consume()
            self.assertTrue(message.value == payload)
        finally:
            consumer.stop()

if __name__ == "__main__":
    unittest2.main()
