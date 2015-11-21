import math
import mock
import time
import unittest2

from kazoo.client import KazooClient

from pykafka import KafkaClient
from pykafka.balancedconsumer import BalancedConsumer, OffsetType
from pykafka.exceptions import NoPartitionsForConsumerException, ConsumerStoppedException
from pykafka.test.utils import get_cluster, stop_cluster
from pykafka.utils.compat import range


def buildMockConsumer(num_partitions=10, num_participants=1, timeout=2000):
    consumer_group = 'testgroup'
    topic = mock.Mock()
    topic.name = 'testtopic'
    topic.partitions = {}
    for k in range(num_partitions):
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
        self._mock_consumer._consumer._partitions_by_id = {1: "dummy"}
        self._mock_consumer._running = True
        start = time.time()
        self._mock_consumer.consume()
        self.assertEqual(int(time.time() - start), int(self._consumer_timeout / 1000))

    def test_consume_graceful_stop(self):
        """Ensure that stopping a consumer while consuming from Kafka does not
        end in an infinite loop when timeout is not used.
        """
        consumer, _ = buildMockConsumer(timeout=-1)
        consumer._setup_internal_consumer(start=False)
        consumer._consumer._partitions_by_id = {1: "dummy"}

        consumer.stop()
        with self.assertRaises(ConsumerStoppedException):
            consumer.consume()

    def test_decide_partitions(self):
        """Test partition assignment for a number of partitions/consumers."""
        # 100 test iterations
        for i in range(100):
            # Set up partitions, cluster, etc
            num_participants = i + 1
            num_partitions = 100 - i
            participants = sorted(['test-debian:{p}'.format(p=p)
                                   for p in range(num_participants)])
            cns, topic = buildMockConsumer(num_partitions=num_partitions,
                                           num_participants=num_participants)

            # Simulate each participant to ensure they're correct
            assigned_parts = []
            for p_id in range(num_participants):
                cns._consumer_id = participants[p_id]  # override consumer id

                # Decide partitions then validate
                partitions = cns._decide_partitions(participants)
                assigned_parts.extend(partitions)

                remainder_ppc = num_partitions % num_participants
                idx = participants.index(cns._consumer_id)
                parts_per_consumer = num_partitions / num_participants
                parts_per_consumer = math.floor(parts_per_consumer)

                num_parts = parts_per_consumer + (0 if (idx + 1 > remainder_ppc) else 1)

                self.assertEqual(len(partitions), int(num_parts))

            # Validate all partitions were assigned once and only once
            all_partitions = topic.partitions.values()
            all_partitions = sorted(all_partitions, key=lambda x: x.id)
            assigned_parts = sorted(assigned_parts, key=lambda x: x.id)
            self.assertListEqual(assigned_parts, all_partitions)


class BalancedConsumerIntegrationTests(unittest2.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.topic_name = b'test-data'
        cls.kafka.create_topic(cls.topic_name, 3, 2)
        cls.client = KafkaClient(cls.kafka.brokers)
        cls.prod = cls.client.topics[cls.topic_name].get_producer(
            min_queued_messages=1
        )
        for i in range(1000):
            cls.prod.produce('msg {num}'.format(num=i).encode())

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def test_consume_earliest(self):
        try:
            consumer_a = self.client.topics[self.topic_name].get_balanced_consumer(
                b'test_consume_earliest', zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.EARLIEST
            )
            consumer_b = self.client.topics[self.topic_name].get_balanced_consumer(
                b'test_consume_earliest', zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.EARLIEST
            )

            # Consume from both a few times
            messages = [consumer_a.consume() for i in range(1)]
            self.assertTrue(len(messages) == 1)
            messages = [consumer_b.consume() for i in range(1)]
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
            try:
                consumer_a.stop()
                consumer_b.stop()
            except:
                pass

    def test_consume_latest(self):
        try:
            consumer_a = self.client.topics[self.topic_name].get_balanced_consumer(
                b'test_consume_latest', zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.LATEST
            )
            consumer_b = self.client.topics[self.topic_name].get_balanced_consumer(
                b'test_consume_latest', zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.LATEST
            )

            # Since we are consuming from the latest offset,
            # produce more messages to consume.
            for i in range(10):
                self.prod.produce('msg {num}'.format(num=i).encode())

            # Consume from both a few times
            messages = [consumer_a.consume() for i in range(1)]
            self.assertTrue(len(messages) == 1)
            messages = [consumer_b.consume() for i in range(1)]
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
            try:
                consumer_a.stop()
                consumer_b.stop()
            except:
                pass

    def test_external_kazoo_client(self):
        """Run with pre-existing KazooClient instance

        This currently doesn't assert anything, it just rules out any trivial
        exceptions in the code path that uses an external KazooClient
        """
        zk = KazooClient(self.kafka.zookeeper)
        zk.start()

        consumer = self.client.topics[self.topic_name].get_balanced_consumer(
            b'test_external_kazoo_client',
            zookeeper=zk,
            consumer_timeout_ms=10)
        [msg for msg in consumer]
        consumer.stop()

    def test_no_partitions(self):
        """Ensure a consumer assigned no partitions immediately exits"""
        consumer = self.client.topics[self.topic_name].get_balanced_consumer(
            b'test_no_partitions',
            zookeeper_connect=self.kafka.zookeeper,
            auto_start=False)
        consumer._decide_partitions = lambda p: set()
        consumer.start()
        self.assertFalse(consumer._running)
        with self.assertRaises(NoPartitionsForConsumerException):
            consumer.consume()

    def test_zk_conn_lost(self):
        """Check we restore zookeeper nodes correctly after connection loss

        See also github issue #204.
        """
        zk = KazooClient(self.kafka.zookeeper)
        zk.start()
        try:
            topic = self.client.topics[self.topic_name]
            consumer_group = b'test_zk_conn_lost'

            consumer = topic.get_balanced_consumer(consumer_group, zookeeper=zk)
            self.assertTrue(consumer._check_held_partitions())
            zk.stop()  # expires session, dropping all our nodes

            # Start a second consumer on a different zk connection
            other_consumer = topic.get_balanced_consumer(consumer_group)

            # Slightly contrived: we'll grab a lock to keep _rebalance() from
            # starting when we restart the zk connection (restart triggers a
            # rebalance), so we can confirm the expected discrepancy between
            # the (empty) set of partitions on zk and the set in the internal
            # consumer:
            with consumer._rebalancing_lock:
                zk.start()
                self.assertFalse(consumer._check_held_partitions())

            # Finally, confirm that _rebalance() resolves the discrepancy:
            time.sleep(.3)  # allow consumers time to begin rebalancing
            with consumer._rebalancing_lock:  # wait until rebalancing finishes
                self.assertTrue(consumer._check_held_partitions())
            with other_consumer._rebalancing_lock:
                self.assertTrue(other_consumer._check_held_partitions())
        finally:
            try:
                consumer.stop()
                other_consumer.stop()
                zk.stop()
            except:
                pass


if __name__ == "__main__":
    unittest2.main()
