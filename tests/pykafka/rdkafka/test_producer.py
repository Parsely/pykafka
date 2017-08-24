import unittest2

from tests.pykafka import test_producer, patch_subclass
try:
    from pykafka.rdkafka import _rd_kafka  # noqa
    RDKAFKA = True
except ImportError:
    RDKAFKA = False  # C extension not built


@patch_subclass(test_producer.ProducerIntegrationTests, not RDKAFKA)
class TestRdKafkaProducer(unittest2.TestCase):
    USE_RDKAFKA = True
