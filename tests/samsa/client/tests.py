import itertools
import mock
import subprocess
import time
import unittest2

from samsa.client import Client, OFFSET_EARLIEST, OFFSET_LATEST
from samsa.test.integration import (KafkaIntegrationTestCase,
    ManagedConsumer, ManagedProducer)


# class ClientTestCase(unittest2.TestCase):
#     def test_produce(self):
#         raise NotImplementedError
#
#     def test_multiproduce(self):
#         raise NotImplementedError
#
#     def test_fetch(self):
#         raise NotImplementedError
#
#     def test_multifetch(self):
#         raise NotImplementedError
#
#     def test_offsets(self):
#         raise NotImplementedError


def filter_messages(stream):
    # TODO: this can deadlock, needs a timeout or something
    prefix = 'consumed: '
    while True:
        line = stream.readline().strip()
        if line.startswith(prefix):
            message = line[len(prefix):]
            yield message


class ClientIntegrationTestCase(KafkaIntegrationTestCase):
    def setUp(self):
        super(ClientIntegrationTestCase, self).setUp()
        self.kafka = Client(host='localhost', port=self.kafka_broker.port)

    def test_produce(self):
        topic = 'topic'
        message = 'hello world'
        consumer = ManagedConsumer(self.hosts, topic)
        self.kafka.produce(topic, 0, (message,))

        consumer.start()

        consumed = next(filter_messages(consumer.process.stdout))
        self.assertEqual(consumed, message)
        consumer.stop()

    def test_multiproduce(self):
        topics = ('topic-a', 'topic-b')

        def message_for_topic(topic):
            return 'hello to topic %s' % topic

        consumers = {}
        for topic in topics:
            consumer = ManagedConsumer(self.hosts, topic)
            consumer.start()
            consumers[topic] = consumer

        batch = []
        for topic in topics:
            batch.append((topic, 0, (message_for_topic(topic),)))

        self.kafka.multiproduce(batch)

        for topic, consumer in consumers.items():
            consumed = next(filter_messages(consumer.process.stdout))
            self.assertEqual(consumed, message_for_topic(topic))
            consumer.stop()

    def test_fetch(self):
        topic = 'topic'
        message = 'hello world'
        size = 1024 * 300

        producer = ManagedProducer(self.hosts, topic)
        producer.start()
        producer.process.stdin.write('%s\n' % message)
        producer.process.stdin.close()
        time.sleep(1)  # TODO: Not this
        producer.stop()

        messages = list(self.kafka.fetch(topic, 0, 0, size))
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0][0], 0)
        self.assertEqual(messages[0][1], message)

    def test_multifetch(self):
        topics = ('topic-a', 'topic-b')
        size = 1024 * 300

        def message_for_topic(topic):
            return 'hello from topic %s' % topic

        for topic in topics:
            producer = ManagedProducer(self.hosts, topic)
            producer.start()
            producer.process.stdin.write('%s\n' % message_for_topic(topic))
            producer.process.stdin.close()
            time.sleep(1)  # TODO: Not this
            producer.stop()

        batches = [(topic, 0, 0, size) for topic in topics]
        responses = self.kafka.multifetch(batches)

        num_responses = 0
        for topic, response in zip(topics, responses):
            messages = list(response)
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0][0], 0)
            self.assertEqual(messages[0][1], message_for_topic(topic))
            num_responses += 1

        self.assertEqual(len(batches), num_responses)

    def test_offsets(self):
        offsets = self.kafka.offsets('topic', 0, OFFSET_EARLIEST, 1)
        self.assertEqual(len(offsets), 1)
        self.assertEqual(offsets[0], 0)
