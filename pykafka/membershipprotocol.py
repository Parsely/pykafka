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
    # Freeze and sort partitions so we always have the same results
    def p_to_str(p):
        return '-'.join([str(p.topic.name), str(p.leader.id), str(p.id)])

    partitions = sorted(partitions.values(), key=p_to_str)
    participants = sorted(participants)

    shorter = partitions if len(partitions) <= len(participants) else participants
    longer = partitions if len(partitions) > len(participants) else participants
    pairs = itertools.izip(longer, itertools.cycle(shorter))

    new_partitions = set()
    for partition, participant in pairs:
        if participant == consumer_id:
            new_partitions.add(partition)
    new_partitions = set(partition for partition, participant in pairs
                         if participant == consumer_id)
    log.info('%s: Balancing %i participants for %i partitions. Owning %i partitions.',
             consumer_id, len(participants), len(partitions),
             len(new_partitions))
    log.debug('My partitions: %s', [p_to_str(p) for p in new_partitions])
    return new_partitions


RoundRobinProtocol = GroupMembershipProtocol(b"consumer",
                                             b"roundrobin",
                                             ConsumerGroupProtocolMetadata(),
                                             decide_partitions_roundrobin)
