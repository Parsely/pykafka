import random


def random_partitioner(partitions, key):
    """
    Returns a random partition out of all of the available partitions.
    """
    return random.choice(list(partitions))


def hashing_partitioner(partitions, key, hash=hash):
    """
    Returns a (relatively) consistent partition out of all available partitions
    based on the key.

    Messages that are published with the same keys are not guaranteed to end up
    on the same broker if the number of brokers changes (due to the addition
    or removal of a broker, planned or unplanned) or if the number of topics
    per partition changes. This is also unreliable when not all brokers are
    aware of a topic, since the number of available partitions will be in flux
    until all brokers have accepted a write to that topic and have declared how
    many partitions that they are actually serving.

    :param partitions: the partitions to choose
    :type partitions: sequence of partitions or
        :class:`samsa.partitions.PartitionMap`
    :param key: key used for routing
    :type key: any hashable type
    :param hash: hash function (defaults to :func:`hash`), should return an
        `int`. If hash randomization (Python 2.7) is enabled, a custom hashing
        function should be defined that is consistent between interpreter
        restarts.
    :type hash: function
    :returns: a partition
    :rtype: :class:`samsa.partitions.Partition`
    """
    if key is None:
        raise ValueError('key cannot be `None` when using hashing partitioner')
    partitions = list(partitions)
    return partitions[abs(hash(key)) % len(partitions)]
