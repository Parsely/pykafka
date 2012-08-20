import random


def random_partitioner(partitions, key):
    """
    Returns a random partition out of all of the available partitions.
    """
    return random.choice(list(partitions))
