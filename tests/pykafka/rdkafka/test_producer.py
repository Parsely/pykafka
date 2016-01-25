import platform

import pytest

from tests.pykafka import test_producer


@pytest.mark.skipif(platform.python_implementation() == "PyPy",
                    reason="We pass PyObject pointers as msg_opaques for "
                           "delivery callbacks, which is unsafe on PyPy.")
class TestRdKafkaProducer(test_producer.ProducerIntegrationTests):
    USE_RDKAFKA = True

    @pytest.mark.xfail
    def test_async_produce_lingers(self):
        """Async produce may not linger

        There seems to be a difference in interpretation between pykafka's
        `linger_ms` and librdkafka's `queue.buffering.max.ms`: in librdkafka,
        it is the longest a message may stick around, but it will be shipped
        sooner if that is expedient.  We cannot, therefore, comply with this
        particular test, but nor do I see practical problems with that.
        """
        super(TestRdKafkaProducer, self).test_async_produce_lingers()

    @pytest.mark.xfail
    def test_recover_disconnected(self):
        """Test fails because we don't use the pykafka.Brokers after start

        (But it won't matter either, because the librdkafka producer does its
        own connection recovery.)
        """
        super(TestRdKafkaProducer, self).test_recover_disconnected()
