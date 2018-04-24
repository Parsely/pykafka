import unittest
import threading
import time
import pytest

from uuid import uuid4

from testinstances.managed_instance import ManagedInstance

from pykafka import KafkaClient, Broker
from pykafka.connection import BrokerConnection
from pykafka.exceptions import SocketDisconnectedError
from pykafka.handlers import ThreadingHandler
from pykafka.utils.compat import itervalues
from pykafka.test.utils import get_cluster, stop_cluster


class TestBrokerConnection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        if not isinstance(cls.kafka, ManagedInstance):
            pytest.skip("Only test on ManagedInstance (run locally)")
        cls.client = KafkaClient(cls.kafka.brokers)

        # BrokerConnection
        ports = cls.kafka._port_generator(9092)
        cls.dest_port = next(ports)
        cls.src_port = next(ports)
        cls.conn = BrokerConnection('localhost',
                                 cls.dest_port,
                                 cls.client._handler,
                                 buffer_size=1024 * 1024,
                                 source_host='localhost',
                                 source_port=cls.src_port,
                                 ssl_config=None)

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def test_connection_fails_no_broker(self):
        """Fail connection when no broker proc exists"""
        assert self.conn is not None
        with self.assertRaises(SocketDisconnectedError):
            self.conn.connect(3 * 1000)

    def test_retry_connect(self):
        """Should retry until the connection succeeds."""
        def delayed_make_broker():
            time.sleep(2)
            self.kafka._start_broker_proc(self.dest_port)

        def retry_connect_broker():
            self.conn.connect(3 * 1000, attempts=20)

        # start broker after delay to ensure initial failure
        delay_make_broker_thread = threading.Thread(target=delayed_make_broker)
        delay_make_broker_thread.start()

        # connect to broker, using retry
        connect_broker_thread = threading.Thread(target=retry_connect_broker)
        connect_broker_thread.start()

        # give enough time for broker creation to complete
        delay_make_broker_thread.join(5)
        connect_broker_thread.join(6)

        assert self.conn.connected is True
