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
__all__ = ["random_partitioner", "BasePartitioner", "HashingPartitioner",
           "hashing_partitioner"]
import random


def random_partitioner(partitions, key):
    """Returns a random partition out of all of the available partitions."""
    return random.choice(partitions)


class BasePartitioner(object):
    """Base class for custom class-based partitioners.

    A partitioner is used by the :class:`pykafka.producer.Producer` to
    decide which partition to which to produce messages.
    """
    def __call__(self, partitions, key=None):
        raise NotImplementedError('Subclasses must define their own '
                                  ' partitioner implementation')


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
    def __init__(self, hash_func=hash):
        """
        :param hash_func: hash function (defaults to :func:`hash`), should return
            an `int`. If hash randomization (Python 2.7) is enabled, a custom
            hashing function should be defined that is consistent between
            interpreter restarts.
        :type hash_func: function
        """
        self.hash_func = hash_func

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
