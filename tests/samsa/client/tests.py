import itertools
import mock
import subprocess
import time
import unittest2

from samsa.client import Client, OFFSET_EARLIEST, OFFSET_LATEST
from samsa.test.integration import KafkaIntegrationTestCase


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
        self.kafka = Client(host='localhost')

    def test_produce(self):
        topic = 'topic'
        consumer = self.consumer(topic)
        message = 'hello world'
        self.kafka.produce(topic, 0, (message,))

        consumed = next(filter_messages(consumer.stdout))
        self.assertEqual(consumed, message)

        consumer.terminate()
        consumer.wait()

    def test_multiproduce(self):
        topics = ('topic-a', 'topic-b')

        def message_for_topic(topic):
            return 'hello to topic %s' % topic

        consumers = {}
        for topic in topics:
            consumers[topic] = self.consumer(topic)

        batch = []
        for topic in topics:
            batch.append((topic, 0, (message_for_topic(topic),)))

        self.kafka.multiproduce(batch)

        for topic, consumer in consumers.items():
            consumed = next(filter_messages(consumer.stdout))
            self.assertEqual(consumed, message_for_topic(topic))
            consumer.terminate()
            consumer.wait()

    def test_fetch(self):
        topic = 'topic'
        message = 'hello world'
        size = 1024 * 300

        producer = self.producer(topic, stdin=subprocess.PIPE)
        producer.stdin.write('%s\n' % message)
        producer.stdin.flush()
        time.sleep(1)  # TODO: Not this
        producer.terminate()
        producer.wait()

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
            producer = self.producer(topic, stdin=subprocess.PIPE)
            producer.stdin.write('%s\n' % message_for_topic(topic))
            producer.stdin.flush()
            time.sleep(1)  # TODO: Not this
            producer.terminate()
            producer.wait()

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
