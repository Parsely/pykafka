"""
Author: Keith Bourgoin, Emmett Butler
"""
__license__ = """
Copyright 2015 Parse.ly, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
__all__ = ["RandomPartitioner", "BasePartitioner", "HashingPartitioner",
           "hashing_partitioner", "GroupHashingPartitioner"]
import random

from hashlib import sha1


class BasePartitioner(object):
    """Base class for custom class-based partitioners.

    A partitioner is used by the :class:`pykafka.producer.Producer` to
    decide which partition to which to produce messages.
    """
    def __call__(self, partitions, key=None):
        raise NotImplementedError('Subclasses must define their own '
                                  ' partitioner implementation')


class RandomPartitioner(BasePartitioner):
    """Returns a random partition out of all of the available partitions.

    Uses a non-random incrementing counter to provide even distribution across partitions
    without wasting CPU cycles
    """
    def __init__(self):
        self.idx = 0

    def __call__(self, partitions, key):
        self.idx = (self.idx + 1) % len(partitions)
        return partitions[self.idx]


class HashingPartitioner(BasePartitioner):
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
    """
    def __init__(self, hash_func=None):
        """
        :param hash_func: hash function (defaults to :func:`hash`), should return
            an `int`. If hash randomization (Python 2.7) is enabled, a custom
            hashing function should be defined that is consistent between
            interpreter restarts.
        :type hash_func: function
        """
        self.hash_func = hash_func
        if self.hash_func is None:
            self.hash_func = lambda k: int(sha1(k).hexdigest(), 16)

    def __call__(self, partitions, key):
        """
        :param partitions: The partitions from which to choose
        :type partitions: sequence of :class:`pykafka.base.BasePartition`
        :param key: Key used for routing
        :type key: Any hashable type if using the default :func:`hash`
            implementation, any valid value for your custom hash function
        :returns: A partition
        :rtype: :class:`pykafka.base.BasePartition`
        """
        if key is None:
            raise ValueError(
                'key cannot be `None` when using hashing partitioner'
            )
        partitions = sorted(partitions)  # sorting is VERY important
        return partitions[abs(self.hash_func(key)) % len(partitions)]


hashing_partitioner = HashingPartitioner()


class GroupHashingPartitioner(BasePartitioner):
    """
    Messages published with the identical keys will be directed to a consistent subset of 'n'
    partitions from the set of available partitions. For example, if there are 16 partitions and group_size=4,
    messages with the identical keys will be shared equally between a subset of four partitions, instead of always
    being directed to the same partition.

    The same guarantee caveats apply as to the :class:`pykafka.base.HashingPartitioner`.
    """
    def __init__(self, hash_func, group_size=1):
        """
        :param hash_func: A hash function
        :type hash_func: function
        :param group_size: Size of the partition group to assign to. For example, if there are 16 partitions, and we
            want to smooth the distribution of identical keys between a set of 4, use 4 as the group_size.
        :type group_size: Integer value between (0, total_partition_count)
        """
        self.hash_func = hash_func
        self.group_size = group_size
        if self.hash_func is None:
            raise ValueError(
                'hash_func must be specified when using GroupHashingPartitioner'
            )
        if self.group_size < 1:
            raise ValueError(
                'group_size cannot be < 1 when using GroupHashingPartitioner'
            )

    def __call__(self, partitions, key):
        """
        :param partitions: The partitions from which to choose
        :type partitions: sequence of :class:`pykafka.base.BasePartition`
        :param key: Key used for routing
        :type key: Any hashable type if using the default :func:`hash`
            implementation, any valid value for your custom hash function
        :returns: A partition
        :rtype: :class:`pykafka.base.BasePartition`
        """
        if key is None:
            raise ValueError(
                'key cannot be `None` when using hashing partitioner'
            )
        if self.group_size > len(partitions):
            raise ValueError(
                'group_size cannot be > available partitions'
            )
        partitions = sorted(partitions)
        return partitions[abs(self.hash_func(key) + random.randrange(0, self.group_size)) % len(partitions)]
