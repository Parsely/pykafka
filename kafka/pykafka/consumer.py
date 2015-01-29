import itertools
from collections import defaultdict
from Queue import Queue

from kafka import base


class SimpleConsumer(base.BaseSimpleConsumer):

    def __init__(self, topic, partitions=None):
        self._consumer_group = None
        self._topic = topic
        if partitions:
            self._partitions = {OwnedPartition(p): topic.partitons[p]
                                for p in partitions}
        else:
            self._partitons = topic.partitions.copy()
        # Organize partitions by broker for efficient queries
        self._partitions_by_broker = defaultdict(list)
        for p in self._partitions.itervalues():
            self._partitions_by_broker[p.broker] = p
        self.partition_cycle = itertools.cycle(self._partitions.values())

    @property
    def topic(self):
        return self._topic

    @property
    def partitions(self):
        return self._partitions

    def __iter__(self):
        while True:
            yield self.consume()

    def consume(self, timeout=None):
        """Get one message from the consumer.

        :param timeout: Seconds to wait before returning None
        """
        pass


class OwnedPartition(object):
    """A partition that is owned by a SimpleConsumer.

    Used to keep track of offsets and the internal message queue.
    """

    def __init__(self, partition):
        self.partition = partition
        self._messages = Queue()

    def consume(self, timeout=None):
        pass
