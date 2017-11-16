from contextlib import contextmanager
import unittest2

import pytest

from pykafka.exceptions import RdKafkaStoppedException, RdKafkaException
try:
    from pykafka.rdkafka import _rd_kafka
    RDKAFKA = True
except ImportError:
    RDKAFKA = False  # C extension not built
from pykafka.test.utils import get_cluster, stop_cluster
from pykafka.utils.compat import get_bytes


@pytest.mark.skipif(not RDKAFKA, reason="C extension for librdkafka not built.")
class TestRdKafkaConsumer(unittest2.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.topic_name = b'test-rdkafka-consumer'
        cls.n_partitions = 3
        cls.kafka.create_topic(cls.topic_name, cls.n_partitions, 2)
        cls.partition_ids = list(range(cls.n_partitions))
        cls.start_offsets = cls.n_partitions * [0]

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    @contextmanager
    def assert_thread_cnt_non_increasing(self):
        """Assert the wrapped function did not leave rdkafka threads behind

        Helper function to check if we correctly clean up rdkafka threads.  We
        can't just check that thread count dropped to zero, because finalisers
        on pypy may run much later than they do on cpython, so that threads
        spawned in previous tests may not have exited yet
        """
        preexisting_threads = _rd_kafka._thread_cnt()
        yield
        try:
            # We could just put `time.sleep(1)` here, but this lets the test
            # finish a bit faster - except on pypy, where it may time out when
            # preexisting_threads is nonzero
            _rd_kafka._wait_destroyed(1000)
        except:
            pass
        self.assertLessEqual(_rd_kafka._thread_cnt(), preexisting_threads)

    def test_start_fail(self):
        """See if Consumer_start cleans up upon failure"""
        with self.assert_thread_cnt_non_increasing():
            consumer = _rd_kafka.Consumer()
            with self.assertRaises(RdKafkaException):
                consumer.start(brokers=b"",  # this causes the exception
                               topic_name=self.topic_name,
                               partition_ids=self.partition_ids,
                               start_offsets=self.start_offsets)

    def test_stop(self):
        """Check Consumer_stop really shuts down the librdkafka consumer

        This is to deal with the fact that librdkafka's _destroy functions are
        all async, and therefore we don't get direct feedback if we didn't
        clean up in the correct order, yet the underlying consumer may remain
        up even if the python object is long gone.  Getting a zero thread
        count in the test gives some reassurance that we didn't leave any
        loose ends.
        """
        with self.assert_thread_cnt_non_increasing():
            consumer = _rd_kafka.Consumer()
            consumer.configure(conf=[])
            consumer.configure(topic_conf=[])
            consumer.start(brokers=get_bytes(self.kafka.brokers),
                           topic_name=self.topic_name,
                           partition_ids=self.partition_ids,
                           start_offsets=self.start_offsets)
            consumer.consume(100)  # just to reliably get some threads going
            consumer.stop()

    def test_stopped_exception(self):
        """Check Consumer_consume raises exception if handle was stopped"""
        consumer = _rd_kafka.Consumer(brokers=get_bytes(self.kafka.brokers),
                                      topic_name=self.topic_name,
                                      partition_ids=self.partition_ids,
                                      start_offsets=self.start_offsets)
        consumer.stop()
        with self.assertRaises(RdKafkaStoppedException):
            consumer.consume(1)
