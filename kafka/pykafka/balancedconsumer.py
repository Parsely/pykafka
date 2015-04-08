import logging as log
from uuid import uuid4
import socket
import itertools

from kazoo.exceptions import NoNodeException
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch

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
        :param zk_host: the ip and port of the zookeeper node to connect to
        :type zk_host: str
        """
        self._cluster = cluster
        self._consumer_group = consumer_group
        self._topic = topic

        self._id_path = '/consumers/{}/ids'.format(self._consumer_group)
        self._id = "{}:{}".format(socket.gethostname(), uuid4())

        self._zookeeper = self._setup_zookeeper(zk_host)
        self._add_self()
        self._consumer = self._setup_internal_consumer()

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
        # Freeze and sort partitions so we always have the same results
        p_to_str = lambda p: '-'.join([p.topic.name, str(p.leader.id)])
        all_partitions = list(self._topic.partitions.values())
        all_partitions.sort(key=p_to_str)

        # get start point, # of partitions, and remainder
        idx = participants.index(self._id)
        parts_per_consumer = len(all_partitions) / len(participants)
        remainder_ppc = len(all_partitions) % len(participants)

        start = parts_per_consumer * idx + min(idx, remainder_ppc)
        num_parts = parts_per_consumer + (0 if (idx + 1 > remainder_ppc) else 1)

        # assign partitions from i*N to (i+1)*N - 1 to consumer Ci
        new_partitions = itertools.islice(
            all_partitions,
            start,
            start + num_parts
        )
        new_partitions = set(new_partitions)
        log.info(
            'Rebalancing %i participants for %i partitions. '
            'My Partitions: %s -- Consumers: %s --- All Partitions: %s',
            len(participants), len(all_partitions),
            [p_to_str(p) for p in new_partitions],
            str(participants),
            [p_to_str(p) for p in all_partitions]
        )
        return new_partitions

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

    def _add_self(self):
        """Add this consumer to the zookeeper participants.

        Ensures we don't add more participants than partitions
        """
        participants = self._get_participants()
        if len(self._topic.partitions) <= len(participants):
            log.debug("More consumers than partitions.")

        path = '{}/{}'.format(self._id_path, self._id)
        self._zookeeper.create(
            path, self._topic.name, ephemeral=True, makepath=True)

        # Set all our watches and then rebalance
        self._rebalancing = False
        broker_path = '/brokers/ids'
        try:
            self._broker_watcher = ChildrenWatch(
                self._zookeeper, broker_path,
                self._brokers_changed
            )
        except NoNodeException:
            raise Exception(
                'The broker_path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?'
                % broker_path)

        self._topics_watcher = ChildrenWatch(
            self._zookeeper,
            '/brokers/topics',
            self._topics_changed
        )
        self._rebalancing = True

        # Final watch will trigger rebalance
        self._consumer_watcher = ChildrenWatch(
            self._zookeeper, self._id_path,
            self._consumers_changed
        )

    def _brokers_changed(self, brokers):
        pass

    def _consumers_changed(self, consumers):
        pass

    def _topics_changed(self, topics):
        pass

    def consume(self):
        """Get one message from the consumer
        """
        return self._consumer.consume()

    def __iter__(self):
        while True:
            yield self._consumer.consume()
