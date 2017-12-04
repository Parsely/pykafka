import itertools
import logging
from collections import namedtuple

from .protocol import ConsumerGroupProtocolMetadata

log = logging.getLogger(__name__)

GroupMembershipProtocol = namedtuple(
    'GroupMembershipProtocol', ['protocol_type',
                                'protocol_name',
                                'metadata',
                                'decide_partitions'])


def decide_partitions_range(participants, partitions, consumer_id):
    """Decide which partitions belong to this consumer_id.

    Uses the consumer rebalancing algorithm described here
    https://kafka.apache.org/documentation/#impl_consumerrebalance

    It is very important that the participants array is sorted,
    since this algorithm runs on each consumer and indexes into the same
    array. The same array index operation must return the same
    result on each consumer.

    :param participants: Sorted list of ids of all consumers in this
        consumer group.
    :type participants: Iterable of `bytes`
    :param partitions: List of all partitions on the topic being consumed
    :type partitions: Iterable of :class:`pykafka.partition.Partition`
    :param consumer_id: The ID of the consumer for which to generate a partition
        assignment.
    :type consumer_id: bytes
    """
    # Freeze and sort partitions so we always have the same results
    def p_to_str(p):
        return '-'.join([str(p.topic.name), str(p.leader.id), str(p.id)])

    all_parts = sorted(partitions.values(), key=p_to_str)

    # get start point, # of partitions, and remainder
    participants = sorted(participants)  # just make sure it's sorted.
    idx = participants.index(consumer_id)
    parts_per_consumer = len(all_parts) // len(participants)
    remainder_ppc = len(all_parts) % len(participants)

    start = parts_per_consumer * idx + min(idx, remainder_ppc)
    num_parts = parts_per_consumer + (0 if (idx + 1 > remainder_ppc) else 1)

    # assign partitions from i*N to (i+1)*N - 1 to consumer Ci
    new_partitions = itertools.islice(all_parts, start, start + num_parts)
    new_partitions = set(new_partitions)
    log.info('%s: Balancing %i participants for %i partitions. Owning %i partitions.',
             consumer_id, len(participants), len(all_parts),
             len(new_partitions))
    log.debug('My partitions: %s', [p_to_str(p) for p in new_partitions])
    return new_partitions


RangeProtocol = GroupMembershipProtocol(b"consumer",
                                        b"range",
                                        ConsumerGroupProtocolMetadata(),
                                        decide_partitions_range)


def decide_partitions_roundrobin(participants, partitions, consumer_id):
    """Decide which partitions belong to this consumer_id.

    Uses the "roundrobin" strategy described here
    https://kafka.apache.org/documentation/#oldconsumerconfigs

    :param participants: Sorted list of ids of all consumers in this
        consumer group.
    :type participants: Iterable of `bytes`
    :param partitions: List of all partitions on the topic being consumed
    :type partitions: Iterable of :class:`pykafka.partition.Partition`
    :param consumer_id: The ID of the consumer for which to generate a partition
        assignment.
    :type consumer_id: bytes
    """
    # Freeze and sort partitions so we always have the same results
    def p_to_str(p):
        return '-'.join([str(p.topic.name), str(p.leader.id), str(p.id)])

    partitions = sorted(partitions.values(), key=p_to_str)
    participants = sorted(participants)

    new_partitions = set()
    partitions_idx = participants_idx = 0
    for _ in range(len(partitions)):
        if participants[participants_idx] == consumer_id:
            new_partitions.add(partitions[partitions_idx])
        partitions_idx = (partitions_idx + 1) % len(partitions)
        participants_idx = (participants_idx + 1) % len(participants)

    log.info('%s: Balancing %i participants for %i partitions. Owning %i partitions.',
             consumer_id, len(participants), len(partitions),
             len(new_partitions))
    log.debug('My partitions: %s', [p_to_str(p) for p in new_partitions])
    return new_partitions


RoundRobinProtocol = GroupMembershipProtocol(b"consumer",
                                             b"roundrobin",
                                             ConsumerGroupProtocolMetadata(),
                                             decide_partitions_roundrobin)
