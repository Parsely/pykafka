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

    def latest_offsets(self):
        return [(p.broker.id, p.latest_offset())
                for p
                in self.partitions]

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

    def subscribe(self,
                  group,
                  backoff_increment=1,
                  connect_retries=4,
                  fetch_size=307200,
                  offset_reset='nearest',
                  rebalance_retries=4,
                  ):
        """
        Returns a new consumer that can be used for reading from this topic.

        `backoff_increment` is used to progressively back off asking a partition
        for messages when there aren't any ready. Incrementally increases wait
        time in seconds.

        `offset_reset` is used to determine where to reset a partition's offset
        in the event of an OffsetOutOfRangeError. Valid values are:

        "earliest": Go to the earliest message in the partition
        "latest": Go to the latest message in the partition
        "nearest": If requested offset is before the earliest, go there,
                   otherwise, go to the latest message in the partition.

        `rebalance_retries` and `connect_retries` affect the number of times
        to try acquiring partitions before giving up.

        When samsa restarts, there can be a bit of lag before
        Zookeeper realizes the old client is dead and releases the partitions
        it was consuming. Setting this means samsa will wait a bit and try to
        acquire partitions again before throwing an error. In the case of
        rebalancing, sometimes it takes a bit for a consumer to release the
        partition they're reading, and this helps account for that.

        :param group: The consumer group to join.
        :param backoff_increment: How fast to incrementally backoff when a
                                  partition has no messages to read.
        :param connect_retries: Retries before giving up on connecting
        :param fetch_size: Default fetch size (in bytes) to get from Kafka
        :param offset_reset: Where to reset when an OffsetOutOfRange happens
        :param rebalance_retries: Retries before giving up on rebalance
        :rtype: :class:`samsa.consumer.consumer.Consumer`
        """
        return Consumer(self.cluster,
                        self,
                        group,
                        backoff_increment=backoff_increment,
                        connect_retries=connect_retries,
                        fetch_size=fetch_size,
                        offset_reset=offset_reset,
                        rebalance_retries=rebalance_retries)
