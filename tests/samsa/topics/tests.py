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

import mock
import unittest2

from samsa import brokers
from samsa.cluster import Cluster
from samsa.exceptions import NoAvailablePartitionsError
from samsa.test.integration import FasterKafkaIntegrationTestCase
from samsa.topics import TopicMap, Topic


class TopicIntgrationTestCase(FasterKafkaIntegrationTestCase):
    def test_no_partitions(self):
        # Kill the broker and try to publish something
        topic = self.kafka_cluster.topics['topic']
        self.kafka_broker.stop()
        with self.assertRaises(NoAvailablePartitionsError):
            topic.publish('message')

        # Restart it afterwards, we're on a shared broker
        self.kafka_broker = self.start_broker(self.client, self.hosts)

    def test_get_latest_offsets(self):
        topic = self.get_topic()
        self.assertEqual(topic.latest_offsets(), [(0, 0)])
        topic.publish(['hello!'])
        self.assertEqual(topic.latest_offsets(), [(0, 15)])


class TopicMapTest(unittest2.TestCase):
    def test_get_topic(self):
        topics = TopicMap(cluster=mock.Mock())
        with mock.patch('samsa.partitions.DataWatch'):
            topic = topics.get('topic-1')
        self.assertIsInstance(topic, Topic)

        # Retrieving the topic again should return the same object instance.
        self.assertIs(topic, topics.get('topic-1'))


class PartitionMapTest(unittest2.TestCase):

    def setUp(self):
        self.cluster = mock.Mock(spec=Cluster)
        self.cluster.zookeeper = mock.Mock()

    @mock.patch('samsa.brokers.ChildrenWatch')
    def test_configuration_no_node(self, cw):
        """Test that we get al the brokers. TODO"""

        broker_map = brokers.BrokerMap(self.cluster)
        self.cluster.brokers = broker_map
        with mock.patch('samsa.brokers.DataWatch'):
            broker_map._configure(['0', '1', '2'])
        with mock.patch('samsa.partitions.DataWatch'):
            topic = Topic(self.cluster, 'topic')

        self.assertEqual(len(topic.partitions), len(broker_map))

    @mock.patch('samsa.brokers.ChildrenWatch')
    def test_configuration_with_nodes(self, cw):
        nodes = {
            '0': '5',
            '1': '2',
            '3': '4',
        }

        def get_node_data(path):
            id = path.rsplit('/', 1)[-1]
            return nodes[str(id)]

        broker_map = brokers.BrokerMap(self.cluster)
        self.cluster.brokers = broker_map
        with mock.patch('samsa.brokers.DataWatch'):
            # Tell the BrokerMap which brokers it's managing.
            broker_map._configure(nodes.keys())

        with mock.patch('samsa.partitions.DataWatch'):
            topic = Topic(self.cluster, 'topic')
            # Tell the PartitionMap which brokers it knows of.
            topic.partitions._configure(nodes.keys())
            # Tell each PartitionSet how many partitions it's managing.
            for partition in topic.partitions._partition_sets:
                partition._configure(
                    nodes[str(partition.broker.id)],
                    mock.Mock()
                )

        self.assertEqual(len(topic.partitions), sum(map(int, nodes.values())))
