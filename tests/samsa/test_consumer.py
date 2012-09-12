__license__ = """
Copyright 2012 DISQUS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging
import mock

from itertools import islice
from kazoo.testing import KazooTestCase

from samsa.test.integration import KafkaIntegrationTestCase, polling_timeout
from samsa.test.case import TestCase
from samsa.cluster import Cluster
from samsa.config import ConsumerConfig
from samsa.topics import Topic
from samsa.partitions import Partition
from samsa.consumer.partitions import PartitionOwnerRegistry, OwnedPartition


logger = logging.getLogger(__name__)


class TestPartitionOwnerRegistry(KazooTestCase):
    """Test the methods of :class:`samsa.consumer.PartitionOwnerRegistry`.
    """

    def setUp(self):
        super(TestPartitionOwnerRegistry, self).setUp()
        self.c = Cluster(self.client)
        self.c.brokers = mock.MagicMock()
        broker = mock.Mock()
        broker.id = 1
        self.c.brokers.__getitem__.return_value = broker

        self.consumer = mock.Mock()
        self.consumer.id  = 1234
        self.topic = mock.Mock()
        self.topic.name = 'topic'

        self.por = PartitionOwnerRegistry(
            self.consumer,
            self.c,
            self.topic,
            'group'
        )

        # Create 5 partitions with on the same topic and broker
        self.partitions = []
        for i in xrange(5):
            self.partitions.append(
                Partition(self.c, self.topic, broker, i)
            )

    def test_crd(self):
        """Test partition *c*reate, *r*ead, and *d*elete.
        """

        # Add the first 3 partitions to the registry and see that they're set
        self.por.add(self.partitions[:3])
        self.assertEquals(
            self.por.get(),
            set(self.partitions[:3])
        )

        # Remove the first partition and see that only [1, 2] exist
        self.por.remove([self.partitions[0]])
        self.assertEquals(
            self.por.get(),
            set(self.partitions[1:3])
        )

    def test_grows(self):
        """Test that the reference returned by
        :func:`samsa.consumer.partitions.PartitionOwnerRegistry.get` reflects
        the latest state.

        """
        partitions = self.por.get()
        self.assertEquals(len(partitions), 0)

        self.por.add(self.partitions)
        self.assertEquals(len(partitions), len(self.partitions))


class TestConsumer(KazooTestCase, TestCase):

    def setUp(self):
        super(TestConsumer, self).setUp()
        self.c = Cluster(self.client)

    def _register_fake_brokers(self, n=1):
        self.client.ensure_path("/brokers/ids")
        for i in xrange(n):
            path = "/brokers/ids/%d" % i
            data = "creator:127.0.0.1:%s" % (9092 + i)
            self.client.create(path, data)

    @mock.patch.object(OwnedPartition, '_fetch')
    def test_assigns_partitions(self, *args):
        """
        Test rebalance

        Adjust n_* to see how rebalancing performs.

        """
        n_partitions = 10
        n_consumers = 3
        self._register_fake_brokers(n_partitions)
        t = Topic(self.c, 'testtopic')

        consumers = [t.subscribe('group1') for i in xrange(n_consumers)]

        partitions = []
        for c in consumers:
            partitions.extend(c.partitions)

        # test that there are no duplicates.
        self.assertEquals(len(partitions), n_partitions)
        # test that every partitions is represented.
        self.assertEquals(len(set(partitions)), n_partitions)

    @mock.patch.object(Partition, 'fetch')
    def test_commits_offsets(self, fetch):
        """Test that message offsets are persisted to ZK.

        """
        self._register_fake_brokers(1)
        t = Topic(self.c, 'testtopic')

        c = t.subscribe('group')
        msgs = []
        for i in xrange(1, 10):
            msg = mock.Mock()
            msg.next_offset = 3 * i
            msg.payload = str(i) * 3
            msgs.append(msg)
        fetch.return_value = msgs

        self.assertPassesWithMultipleAttempts(
            lambda: self.assertTrue(c.next_message(10) is not None),
            5
        )
        self.assertEquals(len(c.partitions), 1)
        p = list(c.partitions)[0]

        self.assertEquals(p.offset, 3)
        c.commit_offsets()

        d, stat = self.client.get(p.path)
        self.assertEquals(d, '3')

    @mock.patch.object(Partition, 'fetch')
    def test_consumer_remembers_offset(self, fetch):
        """Test that offsets are successfully retrieved from zk.

        """
        topic = 'testtopic'
        group = 'testgroup'
        offset = 10

        fake_partition = mock.Mock()
        fake_partition.cluster = self.c
        fake_partition.topic.name = topic
        fake_partition.broker.id = 0
        fake_partition.number = 0
        fetch.return_value = ()

        op = OwnedPartition(fake_partition, group)
        op._current_offset = offset
        op.commit_offset()

        self._register_fake_brokers(1)
        t = Topic(self.c, topic)
        c = t.subscribe(group)

        self.assertEquals(len(c.partitions), 1)
        p = list(c.partitions)[0]
        self.assertEquals(p.offset, offset)

        self.assertEquals(None, p.next_message(0))
        fetch.assert_called_with(offset, ConsumerConfig.fetch_size)


class TestConsumerIntegration(KafkaIntegrationTestCase):

    def setUp(self):
        super(TestConsumerIntegration, self).setUp()
        self.kafka = self.kafka_broker.client

    def test_consumes(self):
        """Test that we can consume messages from kafka.

        """
        topic = 'topic'
        messages = ['hello world', 'foobar']

        # publish `messages` to `topic`
        self.kafka.produce(topic, 0, messages)

        t = Topic(self.kafka_cluster, topic)

        # subscribe to `topic`
        consumer = t.subscribe('group2')

        def test():
            """Test that `consumer` can see `messages`.

            catches exceptions so we can retry while we wait for kafka to
            coallesce.

            """
            logger.debug('Running `test`...')
            try:
                self.assertEquals(
                    list(islice(consumer, 0, len(messages))),
                    messages
                )
                return True
            except AssertionError as e:
                logger.exception('Caught exception: %s', e)
                return False

        # wait for one second for :func:`test` to return true or raise an error
        polling_timeout(test, 1)

        old_offset = [p.offset for p in consumer.partitions][0]
        # test that the offset of our 1 partition is not 0
        self.assertTrue(old_offset > 0)
        # and that consumer contains no more messages.
        self.assertTrue(consumer.empty())

        # repeat and see if offset grows.
        self.kafka.produce(topic, 0, messages)
        polling_timeout(test, 1)
        self.assertTrue([p.offset for p in consumer.partitions][0] > old_offset)


    def test_empty_topic(self):
        """Test that consuming an empty topic returns an empty list.

        """
        topic = 'topic'
        t = Topic(self.kafka_cluster, topic)

        consumer = t.subscribe('group2')
        self.assertTrue(consumer.empty())
