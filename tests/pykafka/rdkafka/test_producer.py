import unittest

from tests.pykafka import test_producer
from pykafka.rdkafka import RdKafkaProducer


class TestRdKafkaProducer(test_producer.ProducerIntegrationTests):

    def _get_producer(self, **kwargs):
        # This enables automatic reuse of all tests from test_producer
        topic = self.client.topics[self.topic_name]
        return RdKafkaProducer(topic._cluster, topic, **kwargs)

    @unittest.skip("Cannot get this to comply")
    def test_async_produce_lingers(self):
        """Async produce may not linger

        This overrides the test by this name in ProducerIntegrationTests.
        There seems to be a difference in interpretation between pykafka's
        `linger_ms` and librdkafka's `queue.buffering.max.ms`: in librdkafka,
        it is the longest a message may stick around, but it will be shipped
        sooner if that is expedient.  We cannot, therefore, comply with this
        particular test, but nor do I see practical problems with that.
        """
        pass
