from tests.pykafka import test_simpleconsumer
from pykafka.rdkafka import RdKafkaSimpleConsumer


class TestRdKafkaSimpleConsumer(test_simpleconsumer.TestSimpleConsumer):
    def _get_simple_consumer(self, **kwargs):
        topic = self.client.topics[self.topic_name]
        return RdKafkaSimpleConsumer(
            topic=topic,
            cluster=topic._cluster,
            **kwargs)
