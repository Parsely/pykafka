import os
import unittest
from uuid import uuid4

import pytest

from pykafka import KafkaClient, SslConfig
from pykafka.test.utils import get_cluster, stop_cluster


kafka_version = os.environ.get('KAFKA_VERSION', '0.8.0')


class SslIntegrationTests(unittest.TestCase):
    USE_RDKAFKA = False

    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        if cls.kafka.brokers_ssl is None:
            pytest.skip("Test-cluster doesn't advertise ssl ports.")

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def roundtrip_test(self, client):
        """Test producing then consuming

        This is mostly important to test the pykafka.rdkafka classes, which
        should be passed SSL settings during producer/consumer init.
        """
        topic_name = uuid4().hex.encode()
        payload = uuid4().hex.encode()
        topic = client.topics[topic_name]

        producer = topic.get_producer(use_rdkafka=self.USE_RDKAFKA, sync=True)
        producer.produce(payload)

        consumer = topic.get_simple_consumer(use_rdkafka=self.USE_RDKAFKA,
                                             consumer_timeout_ms=5000)
        self.assertEqual(consumer.consume().value, payload)

    def test_ca_only(self):
        """Connect with CA cert only (ie no client cert)"""
        config = SslConfig(cafile=self.kafka.certs.root_cert)
        client = KafkaClient(self.kafka.brokers_ssl, ssl_config=config,
                             broker_version=kafka_version)
        self.roundtrip_test(client)

    def test_client_cert(self):
        """Connect with client certificate"""
        # This would be a more potent test if we could on-the-fly reconfigure
        # the test cluster to refuse connections without client certs, but
        # that's not easy to achieve with our current setup
        certs = self.kafka.certs
        config = SslConfig(cafile=certs.root_cert,
                           certfile=certs.client_cert,
                           keyfile=certs.client_key,
                           password=certs.client_pass)
        client = KafkaClient(self.kafka.brokers_ssl, ssl_config=config,
                             broker_version=kafka_version)
        self.roundtrip_test(client)

    @pytest.mark.skip(reason="Unresolved crashes")
    def test_legacy_wrap_socket(self):
        """Test socket-wrapping without SSLContext"""
        config = SslConfig(cafile=self.kafka.certs.root_cert)
        config._wrap_socket = config._legacy_wrap_socket()
        client = KafkaClient(self.kafka.brokers_ssl, ssl_config=config,
                             broker_version=kafka_version)
        self.roundtrip_test(client)
