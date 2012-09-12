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

from samsa.brokers import BrokerMap, Broker
from samsa.cluster import Cluster
from samsa.exceptions import ImproperlyConfiguredError


class BrokerMapTest(unittest2.TestCase):
    def setUp(self):
        self.cluster = mock.Mock(spec=Cluster)
        self.cluster.zookeeper = mock.Mock()

    def test_configuration_no_node(self):
        self.cluster.zookeeper.get_children.side_effect = NoNodeException

        brokers = BrokerMap(self.cluster)
        with self.assertRaises(ImproperlyConfiguredError):
            brokers.get(0)

    def test_initial_configuration(self):
        nodes = ['0', '1', '2', '5']
        self.cluster.zookeeper.get_children.return_value = nodes

        brokers = BrokerMap(self.cluster)
        self.assertEqual(len(brokers), len(nodes))
        self.assertEqual(brokers.keys(), map(int, nodes))
        self.assertTrue(all(isinstance(value, Broker) for value
            in brokers.values()))

        self.assertEqual(self.cluster.zookeeper.get_children.call_count, 1)

    def test_update_configuration(self):
        nodes = ['0', '1']
        self.cluster.zookeeper.get_children.return_value = nodes
        brokers = BrokerMap(self.cluster)
        self.assertEqual(len(brokers), len(nodes))

        broker = brokers.get(1)

        # Emulate a broker entering the pool.
        nodes = ['0', '1', '2']
        self.cluster.zookeeper.get_children.return_value = nodes
        brokers._configure(event=mock.Mock())
        self.assertEqual(len(brokers), len(nodes))

        self.assertIs(broker, brokers.get(1))
        self.assertFalse(broker.is_dead)

        # Emulate a broker leaving the pool.
        nodes = ['0', '2']
        self.cluster.zookeeper.get_children.return_value = nodes
        brokers._configure(event=mock.Mock())
        self.assertEqual(len(brokers), len(nodes))

        self.assertTrue(broker.is_dead)
        with self.assertRaises(KeyError):
            brokers.get(1)


class BrokerTest(unittest2.TestCase):
    def setUp(self):
        self.cluster = mock.Mock(spec=Cluster)
        self.cluster.zookeeper = mock.Mock()

    def test_configuration(self):
        host = 'kafka-1.local'
        port = 9093
        template = '%(host)s-1342221875610:%(host)s:%(port)s'
        self.cluster.zookeeper.get.return_value = (template % {
            'host': host,
            'port': port,
        }, mock.Mock())

        broker = Broker(self.cluster, id_='1')
        self.assertEqual(broker.id, 1)
        self.assertEqual(self.cluster.zookeeper.get.call_count, 0)

        self.assertEqual(broker.host, host)
        self.assertEqual(broker.port, port)
        self.assertEqual(self.cluster.zookeeper.get.call_count, 1)
