import math
import time

import mock
import unittest2

from pykafka import KafkaClient
from pykafka.balancedconsumer import BalancedConsumer
from pykafka.test.utils import get_cluster, stop_cluster


def buildMockConsumer(num_partitions=10, num_participants=1, timeout=2000):
    consumer_group = 'testgroup'
    topic = mock.Mock()
    topic.name = 'testtopic'
    topic.partitions = {}
    for k in xrange(num_partitions):
        part = mock.Mock(name='part-{part}'.format(part=k))
        part.id = k
        part.topic = topic
        part.leader = mock.Mock()
        part.leader.id = k % num_participants
        topic.partitions[k] = part

    cluster = mock.MagicMock()
    zk = mock.MagicMock()
    return BalancedConsumer(topic, cluster, consumer_group,
                            zookeeper=zk, auto_start=False,
                            consumer_timeout_ms=timeout), topic


class TestBalancedConsumer(unittest2.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._consumer_timeout = 2000
        cls._mock_consumer, _ = buildMockConsumer(timeout=cls._consumer_timeout)

    def test_consume_returns(self):
        """Ensure that consume() returns in the amount of time it's supposed to
        """
        self._mock_consumer._setup_internal_consumer(start=False)
        start = time.time()
        self._mock_consumer.consume()
        self.assertEqual(int(time.time() - start), int(self._consumer_timeout / 1000))

    def test_decide_partitions(self):
        """Test partition assignment for a number of partitions/consumers."""
        # 100 test iterations
        for i in xrange(100):
            # Set up partitions, cluster, etc
            num_participants = i + 1
            num_partitions = 100 - i
            participants = ['test-debian:{p}'.format(p=p)
                            for p in xrange(num_participants)]
            cns, topic = buildMockConsumer(num_partitions=num_partitions,
                                           num_participants=num_participants)

            # Simulate each participant to ensure they're correct
            assigned_parts = []
            for p_id in xrange(num_participants):
                cns._consumer_id = participants[p_id]  # override consumer id

                # Decide partitions then validate
                partitions = cns._decide_partitions(participants)
                assigned_parts.extend(partitions)

                remainder_ppc = num_partitions % num_participants
                idx = participants.index(cns._consumer_id)
                parts_per_consumer = num_partitions / num_participants
                parts_per_consumer = math.floor(parts_per_consumer)
                num_parts = parts_per_consumer + (0 if (idx + 1 > remainder_ppc) else 1)

                self.assertEqual(len(partitions), num_parts)

            # Validate all partitions were assigned once and only once
            all_partitions = topic.partitions.values()
            all_partitions.sort()
            assigned_parts.sort()
            self.assertListEqual(assigned_parts, all_partitions)


class BalancedConsumerIntegrationTests(unittest2.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.topic_name = 'test-data'
        cls.kafka.create_topic(cls.topic_name, 3, 2)
        cls.client = KafkaClient(cls.kafka.brokers)
        prod = cls.client.topics[cls.topic_name].get_producer(batch_size=5)
        prod.produce('msg {num}'.format(num=i) for i in xrange(1000))

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def test_consume(self):
        try:
            consumer_a = self.client.topics[self.topic_name].get_balanced_consumer('test_consume', zookeeper_connect=self.kafka.zookeeper)
            consumer_b = self.client.topics[self.topic_name].get_balanced_consumer('test_consume', zookeeper_connect=self.kafka.zookeeper)

            # Consume from both a few times
            messages = [consumer_a.consume() for i in xrange(1)]
            self.assertTrue(len(messages) == 1)
            messages = [consumer_b.consume() for i in xrange(1)]
            self.assertTrue(len(messages) == 1)

            # Validate they aren't sharing partitions
            self.assertSetEqual(
                consumer_a._partitions & consumer_b._partitions,
                set()
            )

            # Validate all partitions are here
            self.assertSetEqual(
                consumer_a._partitions | consumer_b._partitions,
                set(self.client.topics[self.topic_name].partitions.values())
            )
        finally:
            consumer_a.stop()
            consumer_b.stop()


if __name__ == "__main__":
    unittest2.main()
