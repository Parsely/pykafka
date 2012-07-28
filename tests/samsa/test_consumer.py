"""
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

import mock

from kazoo.testing import KazooTestCase

from samsa.test.integration import KafkaIntegrationTestCase, polling_timeout
from samsa.client import Client
from samsa.cluster import Cluster
from samsa.topics import Topic
from samsa.partitions import Partition
from samsa import consumer


class TestPartitionOwnerRegistry(KazooTestCase):

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

        self.por = consumer.PartitionOwnerRegistry(
            self.consumer,
            self.c,
            self.topic,
            'group'
        )

        self.partitions = []
        for i in xrange(5):
            self.partitions.append(
                Partition(self.c, self.topic, broker, i)
            )

    def test_crd(self):
        self.por.add(self.partitions[:3])
        self.assertEquals(
            self.por.get(),
            set(self.partitions[:3])
        )

        self.por.remove([self.partitions[0]])
        self.assertEquals(
            self.por.get(),
            set(self.partitions[1:3])
        )

    def test_grows(self):

        partitions = self.por.get()
        self.assertEquals(len(partitions), 0)

        self.por.add(self.partitions)
        self.assertEquals(len(partitions), len(self.partitions))


class TestConsumer(KazooTestCase):

    def setUp(self):
        super(TestConsumer, self).setUp()
        self.c = Cluster(self.client)

    def _register_fake_brokers(self, n=1):
        self.client.ensure_path("/brokers/ids")
        for i in xrange(n):
            path = "/brokers/ids/%d" % i
            data = "127.0.0.1"
            self.client.create(path, data)

    def test_assigns_partitions(self):
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
        self._register_fake_brokers(1)
        t = Topic(self.c, 'testtopic')

        c = t.subscribe('group')
        fetch.return_value = ((3, "123"),)

        i = list(c)
        c.commit_offsets()

        self.assertEquals(i, ['123'])
        self.assertEquals(len(c.partitions), 1)
        p = list(c.partitions)[0]

        self.assertEquals(p.offset, 3)

        d, stat = self.client.get(p.path)
        self.assertEquals(d, '3')

    @mock.patch.object(Partition, 'fetch')
    def test_consumer_remembers_offset(self, fetch):
        topic = 'testtopic'
        group = 'testgroup'
        offset = 10

        fake_partition = mock.Mock()
        fake_partition.cluster = self.c
        fake_partition.topic.name = topic
        fake_partition.broker.id = 0
        fake_partition.number = 0
        fetch.return_value = ()

        op = consumer.OwnedPartition(fake_partition, group)
        op.offset = offset
        op.commit_offset()

        self._register_fake_brokers(1)
        t = Topic(self.c, topic)
        c = t.subscribe(group)

        self.assertEquals(len(c.partitions), 1)
        p = list(c.partitions)[0]
        self.assertEquals(p.offset, offset)

        self.assertEquals(list(p.fetch(100)), [])
        fetch.assert_called_with(offset, 100)


class TestConsumerIntegration(KafkaIntegrationTestCase):

    def setUp(self):
        super(TestConsumerIntegration, self).setUp()
        self.samsa_cluster = Cluster(self.client)
        self.kafka = Client(host='localhost', port=self.kafka_broker.port)

    def test_consumes(self):
        topic = 'topic'
        messages = ['hello world', 'foobar']
        self.kafka.produce(topic, 0, messages)

        t = Topic(self.samsa_cluster, topic)

        consumer = t.subscribe('group2')

        def test():
            try:
                self.assertEquals(list(consumer), messages)
                return True
            except AssertionError:
                return False

        polling_timeout(test, 1)

        old_offset = [p.offset for p in consumer.partitions][0]
        self.assertTrue(old_offset > 0)
        self.assertEquals(list(consumer), [])

        self.kafka.produce(topic, 0, messages)
        polling_timeout(test, 1)
        self.assertTrue([p.offset for p in consumer.partitions][0] > old_offset)


    def test_empty_topic(self):
        topic = 'topic'
        t = Topic(self.samsa_cluster, topic)

        consumer = t.subscribe('group2')
        self.assertEquals(list(consumer), [])
