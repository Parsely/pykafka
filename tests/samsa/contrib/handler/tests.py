import logging

from samsa.contrib.handler import KafkaHandler
from samsa.test.integration import KafkaIntegrationTestCase


class KafkaHandlerTestCase(KafkaIntegrationTestCase):
    def setUp(self, *args, **kwargs):
        super(KafkaHandlerTestCase, self).setUp(*args, **kwargs)
        self.topic = 'test'
        topic = self.kafka_cluster.topics[self.topic]

        self.logger = logging.getLogger('test')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(KafkaHandler(topic))

    def test_emit(self):
        payload = 'Hello world!'
        self.logger.info(payload)

        def ensure_published():
            client = self.kafka_cluster.brokers[0].client
            messages = list(client.fetch(self.topic, 0, 0, 1000))
            self.assertEqual(len(messages), 1)
            self.assertIn(messages[0].payload, payload)

        self.assertPassesWithMultipleAttempts(ensure_published, 5)
