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
__all__ = ["Topic"]
import logging
from collections import defaultdict

from .balancedconsumer import BalancedConsumer
from .common import OffsetType
from .exceptions import LeaderNotAvailable
from .partition import Partition
from .producer import Producer
from .protocol import PartitionOffsetRequest
from .simpleconsumer import SimpleConsumer
from .utils.compat import iteritems, itervalues


log = logging.getLogger(__name__)


class Topic():
    """
    A Topic is an abstraction over the kafka concept of a topic.
    It contains a dictionary of partitions that comprise it.
    """
    def __init__(self, cluster, topic_metadata):
        """Create the Topic from metadata.

        :param cluster: The Cluster to use
        :type cluster: :class:`pykafka.cluster.Cluster`
        :param topic_metadata: Metadata for all topics.
        :type topic_metadata: :class:`pykafka.protocol.TopicMetadata`
        """
        self._name = topic_metadata.name
        self._cluster = cluster
        self._partitions = {}
        self.update(topic_metadata)

    def __repr__(self):
        return "<{module}.{classname} at {id_} (name={name})>".format(
            module=self.__class__.__module__,
            classname=self.__class__.__name__,
            id_=hex(id(self)),
            name=self._name
        )

    @property
    def name(self):
        """The name of this topic"""
        return self._name

    @property
    def partitions(self):
        """A dictionary containing all known partitions for this topic"""
        return self._partitions

    def get_producer(self, **kwargs):
        """Create a :class:`pykafka.producer.Producer` for this topic.

        For a description of all available `kwargs`, see the Producer docstring.
        """
        return Producer(self._cluster, self, **kwargs)

    def get_sync_producer(self, **kwargs):
        """Create a :class:`pykafka.producer.Producer` for this topic.

        For a description of all available `kwargs`, see the Producer docstring.
        """
        return Producer(self._cluster, self, sync=True, **kwargs)

    def fetch_offset_limits(self, offsets_before, max_offsets=1):
        """Get earliest or latest offset.

        Use the Offset API to find a limit of valid offsets for each partition
        in this topic.

        :param offsets_before: Return an offset from before this timestamp (in
            milliseconds)
        :type offsets_before: int
        :param max_offsets: The maximum number of offsets to return
        :type max_offsets: int
        """
        requests = defaultdict(list)  # one request for each broker
        for part in itervalues(self.partitions):
            requests[part.leader].append(PartitionOffsetRequest(
                self.name, part.id, offsets_before, max_offsets
            ))
        output = {}
        for broker, reqs in iteritems(requests):
            res = broker.request_offset_limits(reqs)
            output.update(res.topics[self.name])
        return output

    def earliest_available_offsets(self):
        """Get the earliest offset for each partition of this topic."""
        return self.fetch_offset_limits(OffsetType.EARLIEST)

    def latest_available_offsets(self):
        """Get the latest offset for each partition of this topic."""
        return self.fetch_offset_limits(OffsetType.LATEST)

    def update(self, metadata):
        """Update the Partitions with metadata about the cluster.

        :param metadata: Metadata for all topics
        :type metadata: :class:`pykafka.protocol.TopicMetadata`
        """
        p_metas = metadata.partitions

        # Remove old partitions
        removed = set(self._partitions.keys()) - set(p_metas.keys())
        if len(removed) > 0:
            log.info('Removing %d partitions', len(removed))
        for id_ in removed:
            log.debug('Removing partition %s', self._partitions[id_])
            self._partitions.pop(id_)

        # Add/update current partitions
        brokers = self._cluster.brokers
        if len(p_metas) > 0:
            log.info("Adding %d partitions", len(p_metas))
        try:
            for id_, meta in iteritems(p_metas):
                if meta.id not in self._partitions:
                    log.debug('Adding partition %s/%s', self.name, meta.id)
                    self._partitions[meta.id] = Partition(
                        self, meta.id,
                        brokers[meta.leader],
                        [brokers[b] for b in meta.replicas],
                        [brokers[b] for b in meta.isr],
                    )
                else:
                    self._partitions[id_].update(brokers, meta)
        except KeyError:
            raise LeaderNotAvailable()

    def get_simple_consumer(self, consumer_group=None, **kwargs):
        """Return a SimpleConsumer of this topic

        :param consumer_group: The name of the consumer group to join
        :type consumer_group: str
        """
        return SimpleConsumer(self, self._cluster,
                              consumer_group=consumer_group, **kwargs)

    def get_balanced_consumer(self, consumer_group, **kwargs):
        """Return a BalancedConsumer of this topic

        :param consumer_group: The name of the consumer group to join
        :type consumer_group: str
        """
        return BalancedConsumer(self, self._cluster, consumer_group, **kwargs)
