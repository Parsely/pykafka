from tests.pykafka import test_producer
from pykafka.rdkafka import RdKafkaProducer


class TestRdKafkaProducer(test_producer.ProducerIntegrationTests):

    def _get_producer(self, **kwargs):
        # This enables automatic reuse of all tests from test_producer
        topic = self.client.topics[self.topic_name]
        return RdKafkaProducer(topic._cluster, topic, **kwargs)
