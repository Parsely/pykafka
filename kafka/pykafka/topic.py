# TODO: Check all __repr__
# TODO: __slots__ where appropriate
# TODO: Use weak refs to avoid reference cycles?

import logging
from collections import defaultdict

from kafka import base
from .partition import Partition
from .producer import Producer
from .protocol import (
    PartitionOffsetRequest, OFFSET_EARLIEST, OFFSET_LATEST
)


logger = logging.getLogger()


class Topic(base.BaseTopic):

    def __init__(self, brokers, topic_metadata):
        """Create the Topic from metadata.

        :param topic_metadata: Metadata for all topics
        :type topic_metadata: :class:`kafka.pykafka.protocol.TopicMetadata`
        """
        self._name = topic_metadata.name
        self._partitions = {}
        self.update(brokers, topic_metadata)

    @property
    def name(self):
        return self._name

    @property
    def partitions(self):
        return self._partitions

    def get_producer(self):
        return Producer(self)

    def fetch_offset_limits(self, offsets_before, max_offsets=1):
        """Use the Offset API to find a limit of valid offsets
            for each partition in this topic

        :param offsets_before: return an offset from before this timestamp (milliseconds)
        :type offsets_before: int
        :param max_offsets: the maximum number of offsets to return
        :type max_offsets: int
        """
        requests = defaultdict(list)  # one request for each broker
        for part in self.partitions.itervalues():
            requests[part.leader].append(PartitionOffsetRequest(
                self.name, part.id, offsets_before, max_offsets
            ))
        output = {}
        for broker, reqs in requests.iteritems():
            res = broker.request_offsets(reqs)
            output.update(res.topics[self.name])
        return output

    def earliest_available_offset(self):
        """Get the earliest offset for each partition of this topic."""
        return self.fetch_offset_limits(OFFSET_EARLIEST)

    def latest_available_offset(self):
        """Get the latest offset for each partition of this topic."""
        return self.fetch_offset_limits(OFFSET_LATEST)

    def update(self, brokers, metadata):
        """Update the Partitons with metadata about the cluster.

        :param brokers: Brokers partitions exist on
        :type brokers: List of :class:`kafka.pykafka.Broker`
        :param metadata: Metadata for all topics
        :type metadata: :class:`kafka.pykafka.protocol.TopicMetadata`
        """
        p_metas = metadata.partitions

        # Remove old partitions
        removed = set(self._partitions.keys()) - set(p_metas.keys())
        for id_ in removed:
            logger.info('Removing partiton %s', self._partitons[id_])
            self._partitons.pop(id_)

        # Add/update current partitions
        for id_, meta in p_metas.iteritems():
            if meta.id not in self._partitions:
                logger.info('Adding partition %s/%s', self.name, meta.id)
                self._partitions[meta.id] = Partition(
                    self, meta.id,
                    brokers[meta.leader],
                    [brokers[b] for b in meta.replicas],
                    [brokers[b] for b in meta.isr],
                )
            else:
                self._partitions[id_].update(meta)
