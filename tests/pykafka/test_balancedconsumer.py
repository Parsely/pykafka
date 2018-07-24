from __future__ import division

import math
import mock
import os
import pkg_resources
import platform
import pytest
import time
import threading
import unittest2
from uuid import uuid4

from kazoo.client import KazooClient
try:
    import gevent
except ImportError:
    gevent = None

from pykafka import KafkaClient
from pykafka.balancedconsumer import BalancedConsumer, OffsetType
from pykafka.exceptions import ConsumerStoppedException
from pykafka.managedbalancedconsumer import ManagedBalancedConsumer
from pykafka.membershipprotocol import (GroupMembershipProtocol, RoundRobinProtocol,
                                        RangeProtocol)
from pykafka.test.utils import get_cluster, stop_cluster
from pykafka.utils.compat import range, iterkeys, iteritems


kafka_version_string = os.environ.get('KAFKA_VERSION', '0.8')
kafka_version = pkg_resources.parse_version(kafka_version_string)
version_09 = pkg_resources.parse_version("0.9.0.0")


class TestBalancedConsumer(unittest2.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._consumer_timeout = 2000
        cls._mock_consumer, _ = TestBalancedConsumer.buildMockConsumer(timeout=cls._consumer_timeout)

    @classmethod
    def buildMockConsumer(self, consumer_group=b'testgroup', num_partitions=10,
                          num_participants=1, timeout=2000):
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
                                zookeeper=zk, auto_start=False, use_rdkafka=False,
                                consumer_timeout_ms=timeout), topic

    def test_unicode_consumer_group(self):
        consumer, _ = self.buildMockConsumer(consumer_group=u'testgroup')

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
        consumer, _ = self.buildMockConsumer(timeout=-1)
        consumer._setup_internal_consumer(start=False)
        consumer._consumer._partitions_by_id = {1: "dummy"}

        consumer.stop()
        with self.assertRaises(ConsumerStoppedException):
            consumer.consume()

    def _test_decide_partitions(self, membership_protocol):
        for i in range(100):
            num_participants = i + 1
            num_partitions = 100 - i
            participants = sorted(['test-debian:{p}'.format(p=p)
                                   for p in range(num_participants)])
            cns, topic = self.buildMockConsumer(num_partitions=num_partitions,
                                                num_participants=num_participants)
            cns._membership_protocol = membership_protocol
            assigned_parts = []
            for consumer_id in participants:
                partitions = cns._membership_protocol.decide_partitions(
                    participants, topic.partitions, consumer_id)
                assigned_parts.extend(partitions)

                remainder_ppc = num_partitions % num_participants
                idx = participants.index(consumer_id)
                parts_per_consumer = math.floor(num_partitions / num_participants)
                num_parts = parts_per_consumer + (0 if (idx + 1 > remainder_ppc) else 1)

                self.assertEqual(len(partitions), int(num_parts))

            # Validate all partitions were assigned once and only once
            all_partitions = sorted(topic.partitions.values(), key=lambda x: x.id)
            assigned_parts = sorted(assigned_parts, key=lambda x: x.id)
            self.assertListEqual(assigned_parts, all_partitions)

    def test_decide_partitions_range(self):
        self._test_decide_partitions(RangeProtocol)

    def test_decide_partitions_roundrobin(self):
        self._test_decide_partitions(RoundRobinProtocol)


class TestManagedBalancedConsumer(TestBalancedConsumer):
    @classmethod
    def buildMockConsumer(self, consumer_group=b'testgroup', num_partitions=10,
                          num_participants=1, timeout=2000):
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
        cns = ManagedBalancedConsumer(topic, cluster, consumer_group,
                                      auto_start=False, use_rdkafka=False,
                                      consumer_timeout_ms=timeout)
        cns._group_coordinator = mock.MagicMock()
        return cns, topic


class BalancedConsumerIntegrationTests(unittest2.TestCase):
    maxDiff = None
    USE_RDKAFKA = False
    USE_GEVENT = False
    MANAGED_CONSUMER = False

    @classmethod
    def setUpClass(cls):
        cls.kafka = get_cluster()
        cls.topic_name = uuid4().hex.encode()
        cls.n_partitions = 3
        cls.kafka.create_topic(cls.topic_name, cls.n_partitions, 2)
        cls.total_msgs = 1000
        cls.client = KafkaClient(cls.kafka.brokers,
                                 use_greenlets=cls.USE_GEVENT,
                                 broker_version=kafka_version_string)
        cls.prod = cls.client.topics[cls.topic_name].get_producer(
            min_queued_messages=1
        )
        for i in range(cls.total_msgs):
            cls.prod.produce('msg {num}'.format(num=i).encode())

    @classmethod
    def tearDownClass(cls):
        stop_cluster(cls.kafka)

    def get_zk(self):
        if not self.USE_GEVENT:
            return KazooClient(self.kafka.zookeeper)

        from kazoo.handlers.gevent import SequentialGeventHandler

        return KazooClient(self.kafka.zookeeper, handler=SequentialGeventHandler())

    def get_balanced_consumer(self, consumer_group, **kwargs):
        if self.MANAGED_CONSUMER:
            kwargs.pop("zookeeper", None)
            kwargs.pop("zookeeper_connect", None)
        return self.client.topics[self.topic_name].get_balanced_consumer(
            consumer_group,
            managed=self.MANAGED_CONSUMER,
            **kwargs
        )

    def test_extra_consumer(self):
        """Ensure proper operation of "extra" consumers in a group

        An "extra" consumer is the N+1th member of a consumer group consuming a topic
        of N partitions, and any consumer beyond the N+1th.
        """
        group = b"test_extra_consumer"
        extras = 1

        def verify_extras(consumers, extras_count):
            messages = [c.consume() for c in consumers]
            successes = [a for a in messages if a is not None]
            nones = [a for a in messages if a is None]
            attempts = 0
            while len(nones) != extras_count and attempts < 5:
                messages = [c.consume() for c in consumers]
                successes = [a for a in messages if a is not None]
                nones = [a for a in messages if a is None]
                attempts += 1
            self.assertEqual(len(nones), extras_count)
            self.assertEqual(len(successes), self.n_partitions)

        try:
            consumers = [self.get_balanced_consumer(group, consumer_timeout_ms=5000)
                         for i in range(self.n_partitions + extras)]
            verify_extras(consumers, extras)

            # when one consumer stops, the extra should pick up its partitions
            removed = consumers[:extras]
            for consumer in removed:
                consumer.stop()
            consumers = [a for a in consumers if a not in removed]
            self.wait_for_rebalancing(*consumers)
            self.assertEqual(len(consumers), self.n_partitions)
            verify_extras(consumers, 0)

            # added "extra" consumers should idle
            for i in range(extras):
                consumers.append(self.get_balanced_consumer(group,
                                                            consumer_timeout_ms=5000))
            self.wait_for_rebalancing(*consumers)
            verify_extras(consumers, extras)
        finally:
            for consumer in consumers:
                try:
                    consumer.stop()
                except:
                    pass

    # weird name to ensure test execution order, because there is an unintended
    # interdependency between test_consume_latest and other tests
    def test_a_rebalance_unblock_event(self):
        """Adding a new consumer instance to a group should release
        blocking consume() call of any existing consumer instance(s).

        https://github.com/Parsely/pykafka/issues/701
        """
        if self.USE_GEVENT:
            pytest.skip("Unresolved failure")
        group = b'test_rebalance'
        consumer_a = self.get_balanced_consumer(group, consumer_timeout_ms=-1)

        # consume all msgs to block the consume() call
        count = 0
        for _ in consumer_a:
            count += 1
            if count == self.total_msgs:
                break

        consumer_a_thread = threading.Thread(target=consumer_a.consume)
        consumer_a_thread.start()

        consumer_b = self.get_balanced_consumer(group, consumer_timeout_ms=-1)
        consumer_b_thread = threading.Thread(target=consumer_b.consume)
        consumer_b_thread.start()

        consumer_a_thread.join(30)
        consumer_b_thread.join(30)

        # consumer thread would die in case of any rebalancing errors
        self.assertTrue(consumer_a_thread.is_alive() and consumer_b_thread.is_alive())

    def test_rebalance_callbacks(self):
        def on_rebalance(cns, old_partition_offsets, new_partition_offsets):
            self.assertTrue(len(new_partition_offsets) > 0)
            self.assigned_called = True
            for id_ in iterkeys(new_partition_offsets):
                new_partition_offsets[id_] = self.offset_reset
            return new_partition_offsets

        self.assigned_called = False
        self.offset_reset = 50
        try:
            consumer_group = b'test_rebalance_callbacks'
            consumer_a = self.get_balanced_consumer(
                consumer_group,
                zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.EARLIEST,
                post_rebalance_callback=on_rebalance,
                use_rdkafka=self.USE_RDKAFKA)
            consumer_b = self.get_balanced_consumer(
                consumer_group,
                zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.EARLIEST,
                use_rdkafka=self.USE_RDKAFKA)
            self.wait_for_rebalancing(consumer_a, consumer_b)
            self.assertTrue(self.assigned_called)
            for _, offset in iteritems(consumer_a.held_offsets):
                self.assertEqual(offset, self.offset_reset)
        finally:
            try:
                consumer_a.stop()
                consumer_b.stop()
            except:
                pass

    def test_rebalance_callbacks_surfaces_errors(self):
        def on_rebalance(cns, old_partition_offsets, new_partition_offsets):
            raise ValueError("BAD CALLBACK")

        self.assigned_called = False
        self.offset_reset = 50
        try:
            consumer_group = b'test_rebalance_callbacks_error'
            consumer_a = self.get_balanced_consumer(
                consumer_group,
                zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.EARLIEST,
                post_rebalance_callback=on_rebalance,
                use_rdkafka=self.USE_RDKAFKA)
            consumer_b = self.get_balanced_consumer(
                consumer_group,
                zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.EARLIEST,
                use_rdkafka=self.USE_RDKAFKA)

            with pytest.raises(ValueError) as ex:
                self.wait_for_rebalancing(consumer_a, consumer_b)
                assert 'BAD CALLBACK' in str(ex.value)

        finally:
            try:
                consumer_a.stop()
                consumer_b.stop()
            except:
                pass

    def test_consume_earliest(self):
        try:
            consumer_a = self.get_balanced_consumer(
                b'test_consume_earliest',
                zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.EARLIEST,
                use_rdkafka=self.USE_RDKAFKA)
            consumer_b = self.get_balanced_consumer(
                b'test_consume_earliest',
                zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.EARLIEST,
                use_rdkafka=self.USE_RDKAFKA)

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
            consumer_a = self.get_balanced_consumer(
                b'test_consume_latest',
                zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.LATEST,
                use_rdkafka=self.USE_RDKAFKA)
            consumer_b = self.get_balanced_consumer(
                b'test_consume_latest',
                zookeeper_connect=self.kafka.zookeeper,
                auto_offset_reset=OffsetType.LATEST,
                use_rdkafka=self.USE_RDKAFKA)

            # Make sure we're done before producing more messages:
            self.wait_for_rebalancing(consumer_a, consumer_b)

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
        if self.MANAGED_CONSUMER:
            pytest.skip("Managed consumer doesn't use zookeeper")
        zk = KazooClient(self.kafka.zookeeper)
        zk.start()

        consumer = self.get_balanced_consumer(
            b'test_external_kazoo_client',
            zookeeper=zk,
            consumer_timeout_ms=10,
            use_rdkafka=self.USE_RDKAFKA)
        [msg for msg in consumer]
        consumer.stop()

    def test_no_partitions(self):
        """Ensure a consumer assigned no partitions doesn't fail"""

        def _decide_dummy(participants, partitions, consumer_id):
            return set()
        consumer = self.get_balanced_consumer(
            b'test_no_partitions',
            zookeeper_connect=self.kafka.zookeeper,
            auto_start=False,
            consumer_timeout_ms=50,
            use_rdkafka=self.USE_RDKAFKA)

        consumer._membership_protocol = GroupMembershipProtocol(
            consumer._membership_protocol.protocol_type,
            consumer._membership_protocol.protocol_name,
            consumer._membership_protocol.metadata,
            _decide_dummy
        )
        consumer.start()
        res = consumer.consume()
        self.assertEqual(res, None)
        self.assertTrue(consumer._running)
        # check that stop() succeeds (cf #313 and #392)
        consumer.stop()

    def test_zk_conn_lost(self):
        """Check we restore zookeeper nodes correctly after connection loss

        See also github issue #204.
        """
        if self.MANAGED_CONSUMER:
            pytest.skip("Managed consumer doesn't use zookeeper")
        check_partitions = lambda c: c._get_held_partitions() == c._partitions
        zk = self.get_zk()
        zk.start()
        try:
            consumer_group = b'test_zk_conn_lost'

            consumer = self.get_balanced_consumer(consumer_group,
                                                  zookeeper=zk,
                                                  use_rdkafka=self.USE_RDKAFKA)
            self.assertTrue(check_partitions(consumer))
            with consumer._rebalancing_lock:
                zk.stop()  # expires session, dropping all our nodes

            # Start a second consumer on a different zk connection
            other_consumer = self.get_balanced_consumer(
                consumer_group, use_rdkafka=self.USE_RDKAFKA)

            # Slightly contrived: we'll grab a lock to keep _rebalance() from
            # starting when we restart the zk connection (restart triggers a
            # rebalance), so we can confirm the expected discrepancy between
            # the (empty) set of partitions on zk and the set in the internal
            # consumer:
            with consumer._rebalancing_lock:
                zk.start()
                self.assertFalse(check_partitions(consumer))

            # Finally, confirm that _rebalance() resolves the discrepancy:
            self.wait_for_rebalancing(consumer, other_consumer)
            self.assertTrue(check_partitions(consumer))
            self.assertTrue(check_partitions(other_consumer))
        finally:
            try:
                consumer.stop()
                other_consumer.stop()
                zk.stop()
            except:
                pass

    def wait_for_rebalancing(self, *balanced_consumers):
        """Test helper that loops while rebalancing is ongoing

        Needs to be given all consumer instances active in a consumer group.
        Waits for up to 100 seconds, which should be enough for even a very
        oversubscribed test cluster.
        """
        for _ in range(500):
            n_parts = [len(cons.partitions) for cons in balanced_consumers]
            if (max(n_parts) - min(n_parts) <= 1
                    and sum(n_parts) == self.n_partitions):
                break
            else:
                balanced_consumers[0]._cluster.handler.sleep(.2)
            # check for failed consumers (there'd be no point waiting anymore)
            [cons._raise_worker_exceptions() for cons in balanced_consumers]
        else:
            raise AssertionError("Rebalancing failed")


@pytest.mark.skipif(platform.python_implementation() == "PyPy" or gevent is None,
                    reason="Unresolved crashes")
class BalancedConsumerGEventIntegrationTests(BalancedConsumerIntegrationTests):
    USE_GEVENT = True


@pytest.mark.skipif(kafka_version < version_09,
                    reason="Managed consumer unsupported until 0.9")
class ManagedBalancedConsumerIntegrationTests(BalancedConsumerIntegrationTests):
    MANAGED_CONSUMER = True


@pytest.mark.skipif(platform.python_implementation() == "PyPy" or
                    kafka_version < version_09 or gevent is None,
                    reason="Unresolved crashes")
class ManagedBalancedConsumerGEventIntegrationTests(BalancedConsumerIntegrationTests):
    MANAGED_CONSUMER = True
    USE_GEVENT = True


if __name__ == "__main__":
    unittest2.main()
