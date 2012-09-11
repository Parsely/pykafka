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

import mock
import unittest2
from kazoo.exceptions import NoNodeException

from samsa.cluster import Cluster
from samsa.exceptions import NoAvailablePartitionsError
from samsa.topics import TopicMap, Topic
from samsa.test.integration import KafkaIntegrationTestCase


class TopicIntgrationTestCase(KafkaIntegrationTestCase):
    def test_no_partitions(self):
        topic = self.kafka_cluster.topics['topic']

        self.kafka_broker.stop()

        with self.assertRaises(NoAvailablePartitionsError):
            topic.publish('message')


class TopicMapTest(unittest2.TestCase):
    def test_get_topic(self):
        topics = TopicMap(cluster=mock.Mock())
        topic = topics.get('topic-1')
        self.assertIsInstance(topic, Topic)

        # Retrieving the topic again should return the same object instance.
        self.assertIs(topic, topics.get('topic-1'))


class PartitionMapTest(unittest2.TestCase):
    def setUp(self):
        self.cluster = Cluster(zookeeper=mock.Mock())

    def test_configuration_no_node(self):
        def get(node, *args, **kwargs):
            if node.startswith('/brokers/ids'):
                return ('::', mock.Mock())
            else:
                raise NoNodeException

        def get_children(node, *args, **kwargs):
            if node.startswith('/brokers/ids'):
                return ['0', '1', '2']
            else:
                raise NoNodeException

        self.cluster.zookeeper.get = get
        self.cluster.zookeeper.get_children = get_children
        self.cluster.zookeeper.exists.return_value = None

        topic = self.cluster.topics.get('topic')
        self.assertEqual(len(topic.partitions), len(self.cluster.brokers))

    def test_configuration_with_nodes(self):
        nodes = {
            '0': '5',
            '1': '2',
            '3': '4',
        }

        def get_node_data(path):
            id = path.rsplit('/', 1)[-1]
            return (nodes[str(id)], mock.Mock())

        self.cluster.zookeeper.get_children.return_value = nodes.keys()
        self.cluster.zookeeper.get = get_node_data

        topic = self.cluster.topics.get('topic')
        self.assertEqual(len(topic.partitions), sum(map(int, nodes.values())))
