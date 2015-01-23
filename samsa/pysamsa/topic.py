# TODO: Check all __repr__
# TODO: __slots__ where appropriate
# TODO: Use weak refs to avoid reference cycles?

import logging
from collections import defaultdict

from samsa import abstract
from samsa.partitioners import random_partitioner
from samsa.protocol import PartitionOffsetRequest, OFFSET_EARLIEST, OFFSET_LATEST
from samsa.utils import attribute_repr

logger = logging.getLogger()

class Topic(abstract.Topic):
    """A topic within a Kafka cluster"""
    def __init__(self, name):
        self.client = None
        self._name = name
        self._partitions = {}

    @property
    def name(self):
        return self._name

    @property
    def partitions(self):
        return self._partitions

    def earliest_offsets(self):
        """Get the earliest offset for each partition of this topic."""
        return self.fetch_offsets(OFFSET_EARLIEST)

    def fetch_offsets(self, offsets_before, max_offsets=1):
        requests = defaultdict(list) # one request for each broker
        for part in self.partitions.itervalues():
            requests[part.leader].append(PartitionOffsetRequest(
                self.name, part.id, offsets_before, max_offsets
            ))
        output = {}
        for broker,reqs in requests.iteritems():
            res = broker.request_offsets(reqs)
            output.update(res.topics[self.name])
        return output

    def latest_offsets(self):
        """Get the latest offset for each partition of this topic."""
        return self.fetch_offsets(OFFSET_LATEST)

    def publish(self, data):
        """Publish one or more messages to a partition of this topic.

        :param data: message(s) to be sent to the broker.
        :type data: ``str`` or sequence of ``str``.
        :param partitioner: callable that takes two arguments, ``partitions``
            and ``key`` and returns a single :class:`~samsa.common.Partition`
            instance to publish the message to.
        :type partitioner: any callable
        :param partition_key: a key to be used for semantic partitioning
        :type partition_key: implementation-specific
        """
        partition = partitioner(self.partitions.values(), partition_key)
        return partition.publish(data)
