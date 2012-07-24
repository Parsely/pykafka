import logging
import socket

from uuid import uuid4

from samsa.utils.delayedconfig import DelayedConfiguration, requires_configuration

logger = logging.getLogger(__name__)


class Consumer(DelayedConfiguration):
    def __init__(self, cluster, topic, group):
        self.cluster = cluster
        self.topic = topic
        self.group = group
        self.id = "%s:%s" % (socket.gethostname(), uuid4())
        self.id_path = '/consumers/%s/ids' % self.group

    def _configure(self, event=None):
        """
        Joins a consumer group and claims partitions.
        """

        path = '%s/%s' % (self.id_path, self.id)
        self.cluster.zookeeper.create(path, self.topic.name, ephemeral=True)

        self._rebalance()

    def _rebalance(self):
        consumer_ids = self.cluster.zk.get_children(self.id_path, watch=self._rebalance)
        participants = []
        for id_ in consumer_ids:
            if id_ == self.id:
                continue
            topic = self.cluster.zk.get("%s/%s" % (self.id_path, id_))
            if topic == self.topic.name:
                participants.append(id_)
        participants.sort()

        self.cluster.zk.get_children('/brokers/ids', watch=self._rebalance)

        n = len(self.topic.partitions) / (len(participants) + 1)
        for i, consumer_id in enumerate(participants):
            for partition in xrange(i * n, (i + 1) * n):
                #assign(consumer_id, partition)
                pass



        """
        partition_id = '%s-%s' % (broker_id, partition_id)
        offset_path = '%s/offsets/%s/%s' % (consumer_path, topic, partition_id)
        owner_registry = '%s/owners/%s/%s-%s' % (consumer_path, topic,
                                                 partition_id)
        """

    @requires_configuration
    def __iter__(self):
        """
        Returns an iterator of messages.
        """
        raise NotImplementedError

    @requires_configuration
    def commit_offsets(self):
        """
        Commit the offsets of all messages consumed so far.
        """

        raise NotImplementedError
