import logging

from samsa import abstract

logger = logging.getLogger(__name__)

class Partition(object):
    def __init__(self, topic, id_, leader, replicas, isr):
        self.id = id_
        self.leader = leader
        self.replicas = replicas
        self.isr = isr
        self.topic = topic

    __repr__ = attribute_repr('topic', 'number')

    def fetch_offsets(self, offsets_before, max_offsets=1):
        request = PartitionOffsetRequest(
            self.topic.name, self.id, offsets_before, max_offsets
        )
        res = self.leader.request_offsets([request])
        return res.topics[self.topic.name]

    def latest_offset(self):
        """Get the latest offset for this partition."""
        return self.fetch_offsets(OFFSET_LATEST)

    def earliest_offset(self):
        """Get the earliest offset for this partition."""
        return self.fetch_offsets(OFFSET_EARLIEST)

    def publish(self,
                data,
                partition_key=None,
                compression_type=compression.NONE,
                required_acks=1,
                timeout=1000):
        """Publish one or more messages to this partition."""
        if isinstance(data, basestring):
            messages = [Message(data, partition_key=partition_key),]
        elif isinstance(data, collections.Sequence):
            messages = [Message(d, partition_key=partition_key) for d in data]
        else:
            raise TypeError('Unable to publish data of type %s' % type(data))

        req = PartitionProduceRequest(self.topic.name, self.id, messages)
        return self.leader.produce_messages(
            [req,], compression_type=compression_type,
            required_acks=required_acks, timeout=timeout
        )

    def fetch_messages(self,
                       offset,
                       timeout=30000,
                       min_bytes=1024,
                       max_bytes=307200):
        req = PartitionFetchRequest(self.topic.name, self.id, offset, max_bytes)
        res = self.leader.fetch_messages(
            [req], timeout=timeout, max_bytes=max_bytes, min_bytes=min_bytes
        )
        return res.topics[self.topic.name].messages

    def __hash__(self):
        return hash((self.topic, self.number))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self == other
