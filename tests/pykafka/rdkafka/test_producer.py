import platform
import unittest

import pytest

from tests.pykafka import test_producer
from pykafka.exceptions import MessageSizeTooLarge
from pykafka.rdkafka import RdKafkaProducer


@pytest.mark.skipif(platform.python_implementation() == "PyPy",
                    reason="We pass PyObject pointers as msg_opaques for "
                           "delivery callbacks, which is unsafe on PyPy.")
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

    def test_msg_too_large(self):
        """Temporarily here until we merge #269 """
        with self._get_producer() as prod:
            with self.assertRaises(MessageSizeTooLarge):
                fut = prod.produce(10**7 * b" ")
                fut.result()
