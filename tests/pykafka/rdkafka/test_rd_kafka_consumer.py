import unittest2

from pykafka.rdkafka import _rd_kafka
from pykafka.test.utils import get_cluster, stop_cluster


class TestRdKafkaConsumer(unittest2.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.topic_name = 'test-rdkafka-consumer'
        cls.n_partitions = 3
        cls.kafka.create_topic(cls.topic_name, cls.n_partitions, 2)
        cls.partition_ids = list(range(cls.n_partitions))
        cls.start_offsets = cls.n_partitions * [0]

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def test_start_fail(self):
        """See if Consumer_start cleans up upon failure"""
        consumer = _rd_kafka.Consumer()
        with self.assertRaises(_rd_kafka.Error):
            consumer.start(brokers="",
                       topic_name=self.topic_name,
                       partition_ids=self.partition_ids,
                       start_offsets=self.start_offsets)
        _rd_kafka._wait_destroyed(1000)
        self.assertEquals(_rd_kafka._thread_cnt(), 0)

    def test_stop(self):
        """Check Consumer_stop really shuts down the librdkafka consumer

        This is to deal with the fact that librdkafka's _destroy functions are
        all async, and therefore we don't get direct feedback if we didn't
        clean up in the correct order, yet the underlying consumer may remain
        up even if the python object is long gone.  Getting a zero thread
        count in the test gives some reassurance that we didn't leave any
        loose ends.
        """
        consumer = _rd_kafka.Consumer()
        consumer.start(brokers=self.kafka.brokers,
                       topic_name=self.topic_name,
                       partition_ids=self.partition_ids,
                       start_offsets=self.start_offsets)

        # We want to check we have some threads at all, because if we don't,
        # then having no threads post-stop() would be nice but meaningless.
        consumer.consume(100)  # just to reliably get some threads going
        self.assertNotEquals(_rd_kafka._thread_cnt(), 0)

        consumer.stop()
        _rd_kafka._wait_destroyed(1000)
        self.assertEquals(_rd_kafka._thread_cnt(), 0)

    def test_stopped_exception(self):
        """Check Consumer_consume raises ConsumerStoppedException"""
        consumer = _rd_kafka.Consumer(brokers=self.kafka.brokers,
                                      topic_name=self.topic_name,
                                      partition_ids=self.partition_ids,
                                      start_offsets=self.start_offsets)
        consumer.stop()
        with self.assertRaises(_rd_kafka.ConsumerStoppedException):
            consumer.consume(1)
