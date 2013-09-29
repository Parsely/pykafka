__license__ = """
Copyright 2012 DISQUS
Copyright 2013 Parse.ly, Inc.

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
import sys
import time
import traceback
import threading
import Queue

from itertools import cycle, islice
from kazoo.testing import KazooTestCase
from threading import Event

from samsa.exceptions import NoAvailablePartitionsError
from samsa.test.integration import KafkaIntegrationTestCase, polling_timeout
from samsa.test.integration import FasterKafkaIntegrationTestCase, polling_timeout
from samsa.test.case import TestCase
from samsa.cluster import Cluster
from samsa.config import ConsumerConfig
from samsa.consumer import Consumer
from samsa.topics import Topic
from samsa.partitions import Partition
from samsa.consumer.partitions import PartitionOwnerRegistry, OwnedPartition


logger = logging.getLogger(__name__)

class TestPartitionOwnerRegistry(KazooTestCase):
    """Test the methods of :class:`samsa.consumer.PartitionOwnerRegistry`.
    """

    @mock.patch('samsa.cluster.BrokerMap')
    def setUp(self, bm, *args):
        super(TestPartitionOwnerRegistry, self).setUp()
        self.c = Cluster(self.client)
        broker = mock.Mock()
        broker.id = 1
        self.c.brokers.__getitem__.return_value = broker

        self.consumer = mock.Mock()
        self.consumer.id  = '1234'
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
        self.message_set_queue = Queue.Queue()
        for i in xrange(5):
            self.partitions.append(
                OwnedPartition(Partition(self.c, self.topic, broker, i), 'group', self.message_set_queue)
            )

    @mock.patch.object(OwnedPartition, 'start')
    def test_crd(self, *args):
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

    @mock.patch.object(OwnedPartition, 'start')
    def test_grows(self, *args):
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
        self.client.ensure_path("/brokers/ids")
        self.client.ensure_path('/brokers/topics')
        self.c = Cluster(self.client)

    def tearDown(self):
        self.client.stop() # stops watches that call _rebalance
        super(TestConsumer, self).tearDown()

    def _register_fake_brokers(self, n=1, client=None):
        if client is None:
            client = self.client
        client.ensure_path("/brokers/ids")
        for i in xrange(n):
            data = "creator:127.0.0.1:%s" % (9092 + i)
            self._register_fake_broker(i, data, client=client)

    def _register_fake_broker(self, name, data, client=None):
        if client is None:
            client = self.client
        path = "/brokers/ids/%s" % name
        client.create(path, data)

    def _register_fake_partitions(self, topic, n_partitions=1, brokers=None, client=None):
        """Register fake partitions. Specify broker list or use None to create for all"""
        if client is None:
            client = self.client
        brokers = brokers or client.get_children("/brokers/ids")
        for broker in brokers:
            client.ensure_path("/brokers/topics/%s" % topic)
            part_path = "/brokers/topics/%s/%s" % (topic, broker)
            client.create(part_path, str(n_partitions))


    @mock.patch.object(OwnedPartition, 'start')
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


    @mock.patch.object(OwnedPartition, 'start')
    def test_broker_addition(self, rebalance):
        """Test adding a broker, and ensure all partitions are discovered

        Testing this makes sure all the relevant zookeeper watches are
        functioning and rebalancing is happening when partitions or brokers
        are added or removed
        """
        t = Topic(self.c, 'testtopic')

        self._register_fake_broker(0, "creator:127.0.0.1:9092")
        self._register_fake_partitions('testtopic', n_partitions=2)

        consumer = t.subscribe('group1')

        self._register_fake_broker(1, "creator:127.0.0.1:9093")
        self._register_fake_partitions('testtopic', n_partitions=2, brokers=['1'])

        time.sleep(1) # let watches resolve
        self.assertEqual(len(t.partitions), 4)
        self.assertEqual(len(consumer.partitions), 4)


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
        c.stop_partitions()

    @mock.patch.object(Partition, 'fetch')
    def test_consumer_remembers_offset(self, fetch):
        """Test that offsets are successfully retrieved from zk.

        """
        return # TODO: Fix this test
        topic = 'testtopic'
        group = 'testgroup'
        offset = 10
        ev = Event()

        fake_partition = mock.Mock()
        fake_partition.cluster = self.c
        fake_partition.topic.name = topic
        fake_partition.broker.id = 0
        fake_partition.number = 0
        def fake_fetch(*args, **kwargs):
            ev.set()
            return ()
        fetch.side_effect = fake_fetch

        msgqueue = Queue.Queue()
        op = OwnedPartition(fake_partition, group, msgqueue)
        op._current_offset = offset
        op.commit_offset()

        self._register_fake_brokers(1)
        t = Topic(self.c, topic)
        c = t.subscribe(group)

        self.assertEquals(len(c.partitions), 1)
        p = list(c.partitions)[0]
        self.assertEquals(p.offset, offset)

        self.assertEquals(None, p.next_message(0))
        ev.wait(1)
        fetch.assert_called_with(offset, ConsumerConfig.fetch_size)
        c.stop_partitions()


    @mock.patch.object(OwnedPartition, 'start')
    def test_multiclient_rebalance(self, *args):
        """Test rebalancing with many connected clients

        This test is primarily good at ferreting out concurrency bugs
        and therefore doesn't test one specific thing, since such
        bugs are fundamentally hard to trap.

        To test, it simulates 10 consumer connecting over time, rebalancing,
        and eventually disconnecting one by one. In order to accomplish
        this, it creates a separate kazoo client for each consumer. This
        is required because otherwise there is too much thread contention
        over the single kazoo client. Some callbacks won't happen until
        much too late because there aren't enough threads to go around.

        """
        n_partitions = 10
        n_consumers = 10
        t = Topic(self.c, 'testtopic')

        # with self.client, new clients don't see the brokers -- unsure why
        from kazoo.client import KazooClient
        zk_hosts = ','.join(
                ['%s:%s' % (h,p) for h,p in self.client.hosts.hosts])
        zkclient = KazooClient(hosts=zk_hosts)
        zkclient.start()
        self._register_fake_brokers(n_partitions, client=zkclient)
        zkclient.ensure_path('/brokers/topics')

        # bring up consumers
        consumers = []
        for i in xrange(n_consumers):
            newclient = KazooClient(hosts=zk_hosts)
            newclient.start()
            cluster = Cluster(newclient)
            topic = Topic(cluster, 'testtopic')
            consumers.append((newclient, topic.subscribe('group1')))
            time.sleep(1)

        time.sleep(5) # let things settle

        # bring down consumers
        for client,consumer in consumers:
            consumer.stop_partitions()
            client.stop()
            time.sleep(2) # a little more time so we don't kill during rebalance

        newclient.stop()

    @mock.patch.object(OwnedPartition, 'start')
    def test_too_many_consumers(self, *args):
        """Test graceful failure when # of consumers exceeds partitions

        """
        n_partitions = 1
        n_consumers = 2
        self._register_fake_brokers(n_partitions)
        t = Topic(self.c, 'testtopic')

        with self.assertRaises(NoAvailablePartitionsError):
            consumers = [t.subscribe('group1') for i in xrange(n_consumers)]


class TestConsumerIntegration(FasterKafkaIntegrationTestCase):

    def setUp(self):
        super(TestConsumerIntegration, self).setUp()
        self.kafka = self.kafka_broker.client
        self.client.ensure_path('/brokers/topics') # can be slow to create


    def test_consumes(self):
        """Test that we can consume messages from kafka.

        """
        topic = self.get_topic().name
        messages = ['hello world', 'foobar']

        # publish `messages` to topic
        self.kafka.produce(topic, 0, messages)

        t = Topic(self.kafka_cluster, topic)

        # subscribe to topic
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
        consumer.stop_partitions()


    def test_empty_topic(self):
        """Test that consuming an empty topic returns an empty list.

        """
        topic = self.get_topic().name
        t = Topic(self.kafka_cluster, topic)

        consumer = t.subscribe('group2')
        self.assertTrue(consumer.empty())
        consumer.stop_partitions()


    def test_fetch_invalid_offset(self):
        """Test that fetching rolled-over offset skips to lowest valid offset.

        Bad offsets happen when kafka logs are rolled over automatically.
        This could happen when a consumer is offline for a long time and
        zookeeper has an old offset stored. It could also happen with a
        consumer near the end of a log that's being rolled over and its
        previous spot no longer exists.
        """
        topic = self.get_topic().name
        messages = ['hello world', 'foobar']

        # publish `messages` to topic
        self.kafka.produce(topic, 0, messages)

        t = Topic(self.kafka_cluster, topic)

        # get the consumer and set the offset to -1
        consumer = t.subscribe('group2')
        list(consumer.partitions)[0]._next_offset = -1

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
        consumer.stop_partitions()
