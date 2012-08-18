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

from samsa.handlers import ThreadingHandler
from samsa.brokers import BrokerMap
from samsa.topics import TopicMap


class Cluster(object):
    """
    A Kafka cluster.

    :ivar brokers: The :class:`samsa.brokers.BrokerMap` for this cluster.
    :ivar topics: The :class:`samsa.topics.TopicMap` for this cluster.

    :param zookeeper: A ZooKeeper client.
    :type zookeeper: :class:`kazoo.client.Client`
    :param handler: Async handler.
    :type handler: :class:`kazoo.handlers.Handler`
    """
    def __init__(self, zookeeper, handler=None):
        self.zookeeper = zookeeper

        if not handler:
            handler = ThreadingHandler()
        self.handler = handler

        self.brokers = BrokerMap(self)
        self.topics = TopicMap(self)
