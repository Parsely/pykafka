import logging as log

from kazoo.exceptions import NoNodeException
from kazoo.client import KazooClient

from kafka.pykafka.simpleconsumer import SimpleConsumer


class BalancedConsumer():
    def __init__(self,
                 topic,
                 cluster,
                 consumer_group,
                 zk_host='127.0.0.1:2181'):
        """Create a BalancedConsumer

        :param topic: the topic this consumer should consume
        :type topic: pykafka.topic.Topic
        :param cluster: the cluster this consumer should connect to
        :type cluster: pykafka.cluster.Cluster
        :param consumer_group: the name of the consumer group to join
        :type consumer_group: str
        """
        self._cluster = cluster
        self._consumer_group = consumer_group
        self._topic = topic

        self._zookeeper = self._setup_zookeeper(zk_host)
        self._consumer = self._setup_internal_consumer()

        self._id_path = '/consumers/{}/ids'.format(self.consumer_group)

    def _setup_zookeeper(self, zk_host):
        zk = KazooClient(zk_host)
        zk.start()
        return zk

    def _setup_internal_consumer(self):
        participants = self._get_participants()
        partitions = self._decide_partitions(participants)
        return SimpleConsumer(self._topic,
                              self._cluster,
                              consumer_group=self._consumer_group,
                              partitions=partitions)

    def _decide_partitions(self, participants):
        return []

    def _get_participants(self):
        """Use zookeeper to get the other consumers of this topic
        """
        try:
            consumer_ids = self._zookeeper.get_children(self._id_path)
        except NoNodeException:
            log.debug("Consumer group doesn't exist. "
                      "No participants to find")
            return []

        participants = []
        for id_ in consumer_ids:
            try:
                topic, stat = self._zookeeper.get("%s/%s" % (self._id_path, id_))
                if topic == self._topic.name:
                    participants.append(id_)
            except NoNodeException:
                pass  # disappeared between ``get_children`` and ``get``
        participants.sort()
        return participants
