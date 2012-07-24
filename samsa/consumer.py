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
        self.consumer_id = "%s:%s" % (socket.gethostname, uuid4())

    def _configure(self, event=None):
        """
        Joins a consumer group and claims partitions.
        """

        path = '/brokers/ids'
        logger.info('Refreshing broker configuration from %s...', self.cluster.zookeeper)

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
