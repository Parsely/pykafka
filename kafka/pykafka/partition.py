import logging

from kafka import base
from kafka.common import OffsetType
from .protocol import PartitionOffsetRequest

logger = logging.getLogger(__name__)


class Partition(base.BasePartition):
    def __init__(self, topic, id_, leader, replicas, isr):
        self._id = id_
        self._leader = leader
        self._replicas = replicas
        self._isr = isr
        self._topic = topic

    @property
    def id(self):
        return self._id

    @property
    def leader(self):
        return self._leader

    @property
    def replicas(self):
        return self._replicas

    @property
    def isr(self):
        return self._isr

    @property
    def topic(self):
        return self._topic

    def fetch_offset(self, consumer_group):
        """Use the Offset Commit/Fetch API to get the current offset for this
            partition for the given consumer group

        :param consumer_group: the name of the consumer group for which to
            fetch an offset
        :type consumer_group: str
        """
        pass

    def commit_offset(self, consumer_group):
        """Use the Offset Commit/Fetch API to set the current offset for this
            partition for the given consumer group

        :param consumer_group: the name of the consumer group for which to
            fetch an offset
        :type consumer_group: str
        """
        pass

    def fetch_offset_limit(self, offsets_before, max_offsets=1):
        """Use the Offset API to find a limit of valid offsets
            for this partition

        :param offsets_before: return an offset from before this timestamp (milliseconds)
        :type offsets_before: int
        :param max_offsets: the maximum number of offsets to return
        :type max_offsets: int
        """
        request = PartitionOffsetRequest(
            self.topic.name, self.id, offsets_before, max_offsets
        )
        res = self.leader.request_offsets([request])
        return res.topics[self.topic.name][self._id][0]

    def latest_available_offset(self):
        """Get the latest offset for this partition."""
        return self.fetch_offset_limit(OffsetType.LATEST)[self._id][0]

    def earliest_available_offset(self):
        """Get the earliest offset for this partition."""
        return self.fetch_offset_limit(OffsetType.EARLIEST)[self._id][0]

    def __hash__(self):
        return hash((self.topic, self.id))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self == other

    def update(self, brokers, metadata):
        """Update partition with fresh metadata.

        :param brokers: Brokers partitions exist on
        :type brokers: List of :class:`kafka.pykafka.Broker`
        :param metadata: Metadata for the partition
        :type metadata: :class:`kafka.pykafka.protocol.PartitionMetadata`
        """
        try:
            # Check leader
            if metadata.leader != self.leader.id:
                logger.info('Updating leader for %s', self)
                self.leader = brokers[metadata.leader]
            # Check Replicas
            if sorted(r.id for r in self.replicas) != sorted(metadata.replicas):
                logger.info('Updating replicas list for %s', self)
                self.replicas = [brokers[b] for b in metadata.replicas]
            # Check In-Sync-Replicas
            if sorted(i.id for i in self.isr) != sorted(metadata.isr):
                logger.info('Updating in sync replicas list for %s', self)
                self.isr = [brokers[b] for b in metadata.isr]
        except KeyError:
            raise Exception("TODO: Type this exception")
