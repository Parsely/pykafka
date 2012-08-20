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
import random

from samsa.exceptions import NoAvailablePartitionsError
from samsa.partitioners import random_partitioner
from samsa.partitions import PartitionMap
from samsa.consumer import Consumer
from samsa.utils import attribute_repr


logger = logging.getLogger(__name__)


class TopicMap(object):
    """
    Provides a dictionary-like interface to :class:`~samsa.topics.Topic`
    instances within a cluster.

    :param cluster: The cluster this topic mapping is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    """
    def __init__(self, cluster):
        self.cluster = cluster

        self.__topics = {}

    def __getitem__(self, key):
        """
        Returns a :class:`samsa.topics.Topic` for the given key.

        This is a proxy to :meth:`~TopicMap.get` for a more dict-like
        interface.
        """
        return self.get(key)

    def get(self, name):
        """
        Returns a :class:`samsa.topics.Topic` for this topic name, creating a
        new topic if one has not already been registered.
        """
        topic = self.__topics.get(name, None)
        if topic is None:
            topic = self.__topics[name] = Topic(self.cluster, name)
            logger.info('Registered new topic: %s', topic)
        return topic


class Topic(object):
    """
    A topic within a Kafka cluster.

    :param cluster: The cluster that this topic is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    :param name: The name of this topic.
    :param partitioner: callable that takes two arguments, ``partitions`` and
        ``key`` and returns a single :class:`~samsa.partitions.Partition`
        instance to publish the message to.
    :type partitioner: any callable type
    """
    def __init__(self, cluster, name, partitioner=random_partitioner):
        self.cluster = cluster
        self.name = name
        self.partitions = PartitionMap(self.cluster, self)
        self.partitioner = partitioner

    __repr__ = attribute_repr('name')

    def publish(self, data, key=None):
        """
        Publishes one or more messages to a random partition of this topic.

        :param data: message(s) to be sent to the broker.
        :type data: ``str`` or sequence of ``str``.
        :param key: a key to be used for semantic partitioning
        :type key: implementation-specific
        """
        if len(self.partitions) < 1:
            raise NoAvailablePartitionsError('No partitions are available to '
                'accept a write for this message. (Is your Kafka broker '
                'running?)')
        partition = self.partitioner(self.partitions, key)
        return partition.publish(data)

    def subscribe(self, group):
        """
        Returns a new consumer that can be used for reading from this topic.

        :param group: the name of the consumer group this consumer belongs to
        :type group: ``str``
        :rtype: :class:`samsa.consumer.consumer.Consumer`
        """
        return Consumer(self.cluster, self, group)
