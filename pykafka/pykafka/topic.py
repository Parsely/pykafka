# TODO: Check all __repr__
# TODO: __slots__ where appropriate
# TODO: Use weak refs to avoid reference cycles?

from collections import defaultdict
import logging
import weakref

from pykafka import base
from pykafka.common import OffsetType
from pykafka.balancedconsumer import BalancedConsumer
from .partition import Partition
from .producer import Producer
from .protocol import PartitionOffsetRequest
from .simpleconsumer import SimpleConsumer


logger = logging.getLogger()


class Topic(base.BaseTopic):

    def __init__(self, cluster, topic_metadata):
        """Create the Topic from metadata.

        :param topic_metadata: Metadata for all topics
        :type topic_metadata: :class:`kafka.pykafka.protocol.TopicMetadata`
        """
        self._name = topic_metadata.name
        self._cluster = weakref.proxy(cluster)
        self._partitions = {}
        self.update(topic_metadata)

    @property
    def name(self):
        return self._name

    @property
    def partitions(self):
        return self._partitions

    def get_producer(self):
        return Producer(self._cluster, self)

    def fetch_offset_limits(self, offsets_before, max_offsets=1):
        """Get earliest or latest offset

        Use the Offset API to find a limit of valid offsets for each partition
        in this topic

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

    def earliest_available_offsets(self):
        """Get the earliest offset for each partition of this topic."""
        return self.fetch_offset_limits(OffsetType.EARLIEST)

    def latest_available_offsets(self):
        """Get the latest offset for each partition of this topic."""
        return self.fetch_offset_limits(OffsetType.LATEST)

    def update(self, metadata):
        """Update the Partitons with metadata about the cluster.

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
        brokers = self._cluster.brokers
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
                self._partitions[id_].update(brokers, meta)

    def get_simple_consumer(self, consumer_group=None, **kwargs):
        """Return a SimpleConsumer of this topic
        """
        return SimpleConsumer(self, self._cluster,
                              consumer_group=consumer_group, **kwargs)

    def get_balanced_consumer(self, consumer_group, **kwargs):
        """Return a BalancedConsumer of this topic
        """
        return BalancedConsumer(self, self._cluster, consumer_group, **kwargs)
