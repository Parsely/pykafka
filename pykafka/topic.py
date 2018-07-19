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
import datetime as dt
import logging
from collections import defaultdict

from .balancedconsumer import BalancedConsumer
from .common import OffsetType, EPOCH
from .exceptions import LeaderNotFoundError
from .managedbalancedconsumer import ManagedBalancedConsumer
from .partition import Partition
from .producer import Producer
from .protocol import PartitionOffsetRequest
from .simpleconsumer import SimpleConsumer
from .utils.compat import iteritems, itervalues
try:
    from .handlers import GEventHandler
except ImportError:
    GEventHandler = None


log = logging.getLogger(__name__)


try:
    from . import rdkafka
    log.info("Successfully loaded pykafka.rdkafka extension.")
except ImportError:
    rdkafka = False
    log.info("Could not load pykafka.rdkafka extension.")
    log.debug("Traceback:", exc_info=True)


class Topic(object):
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

    def get_producer(self, use_rdkafka=False, **kwargs):
        """Create a :class:`pykafka.producer.Producer` for this topic.

        For a description of all available `kwargs`, see the Producer docstring.
        """
        if not rdkafka and use_rdkafka:
            raise ImportError("use_rdkafka requires rdkafka to be installed")
        if GEventHandler and isinstance(self._cluster.handler, GEventHandler) and use_rdkafka:
            raise ImportError("use_rdkafka cannot be used with gevent")
        Cls = Producer
        if rdkafka and use_rdkafka:
            Cls = rdkafka.RdKafkaProducer
            kwargs.pop('block_on_queue_full', None)
        return Cls(self._cluster, self, **kwargs)

    def get_sync_producer(self, **kwargs):
        """Create a :class:`pykafka.producer.Producer` for this topic.

        The created `Producer` instance will have `sync=True`.

        For a description of all available `kwargs`, see the Producer docstring.
        """
        return Producer(self._cluster, self, sync=True, **kwargs)

    def fetch_offset_limits(self, offsets_before, max_offsets=1):
        """Get information about the offsets of log segments for this topic

        The ListOffsets API, which this function relies on, primarily deals
        with topics in terms of their log segments. Its behavior can be summed
        up as follows: it returns some subset of starting message offsets for
        the log segments of each partition. The particular subset depends on
        this function's two arguments, filtering by timestamp and in certain
        cases, count. The documentation for this API is notoriously imprecise,
        so here's a little example to illustrate how it works.

        Take a topic with three partitions 0,1,2. 2665 messages have been
        produced to this topic, and the brokers' `log.segment.bytes` settings
        are configured such that each log segment contains roughly 530
        messages. The two oldest log segments have been deleted due to log retention
        settings such as `log.retention.hours`. Thus, the `log.dirs` currently
        contains these files for partition 0:

        /var/local/kafka/data/test2-0/00000000000000001059.log
        /var/local/kafka/data/test2-0/00000000000000002119.log
        /var/local/kafka/data/test2-0/00000000000000001589.log
        /var/local/kafka/data/test2-0/00000000000000002649.log

        The numbers on these filenames indicate the offset of the earliest
        message contained within. The most recent message was written at 1523572215.69.

        Given this log state, a call to this function with
        offsets_before=OffsetType.LATEST
        and max_offsets=100 will result in a return value of
        [2665,2649,2119,1589,1059] for partition 0. The first value (2665) is
        the offset of the latest available message from the latest log
        segment. The other four offsets are those of the earliest messages
        from each log segment for the partition. Changing max_offsets to
        3 will result in only the first three elements of this list being returned.

        A call to this function with offsets_before=OffsetType.EARLIEST will
        result in a value of [1059] - only the offset of the earliest message
        present in log segments for partition 0. In this case, the return
        value is not affected by max_offsets.

        A call to this function with offsets_before=(1523572215.69 * 1000)
        (the timestamp in milliseconds of the very last message written to the
        partition) will result in a value of [2649,2119,1589,1059]. This is
        the same list as with OffsetType.LATEST, but with the first element
        removed. This is because unlike the other elements, the message with
        this offset (2665) was not written *before* the given timestamp.

        In cases where there are no log segments fitting the given criteria
        for a partition, an empty list is returned. This applies if the given
        timestamp is before the write time of the oldest message in the
        partition, as well as if there are no log segments for the partition.

        Thanks to Andras Beni from the Kafka users mailing list for providing
        this example.

        :param offsets_before: Epoch timestamp in milliseconds or datetime indicating the
            latest write time for returned offsets. Only offsets of messages
            written before this timestamp will be returned. Permissible
            special values are `common.OffsetType.LATEST`, indicating that
            offsets from all available log segments should be returned, and
            `common.OffsetType.EARLIEST`, indicating that only the offset of
            the earliest available message should be returned. Deprecated::2.7,3.6:
            do not use int
        :type offsets_before: `datetime.datetime` or int
        :param max_offsets: The maximum number of offsets to return when more
            than one is available. In the case where `offsets_before ==
            OffsetType.EARLIEST`, this parameter is meaningless since there is
            always either one or zero earliest offsets. In other cases, this
            parameter slices off the earliest end of the list, leaving the
            latest `max_offsets` offsets.
        :type max_offsets: int
        """
        if isinstance(offsets_before, dt.datetime):
            offsets_before = round((offsets_before - EPOCH).total_seconds() * 1000)
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
        """Fetch the next available offset

        Get the offset of the next message that would be appended to each partition of
            this topic.
        """
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
        for id_, meta in iteritems(p_metas):
            if meta.leader not in brokers:
                raise LeaderNotFoundError()
            if meta.id not in self._partitions:
                log.debug('Adding partition %s/%s', self.name, meta.id)
                self._partitions[meta.id] = Partition(
                    self, meta.id,
                    brokers[meta.leader],
                    # only add replicas that the cluster is aware of to avoid
                    # KeyErrors here. inconsistencies will be automatically
                    # resolved when `Cluster.update` is called.
                    [brokers[b] for b in meta.replicas if b in brokers],
                    [brokers[b] for b in meta.isr],
                )
            else:
                self._partitions[id_].update(brokers, meta)

    def get_simple_consumer(self,
                            consumer_group=None,
                            use_rdkafka=False,
                            **kwargs):
        """Return a SimpleConsumer of this topic

        :param consumer_group: The name of the consumer group to join
        :type consumer_group: bytes
        :param use_rdkafka: Use librdkafka-backed consumer if available
        :type use_rdkafka: bool
        """
        if not rdkafka and use_rdkafka:
            raise ImportError("use_rdkafka requires rdkafka to be installed")
        if GEventHandler and isinstance(self._cluster.handler, GEventHandler) and use_rdkafka:
            raise ImportError("use_rdkafka cannot be used with gevent")
        Cls = (rdkafka.RdKafkaSimpleConsumer
               if rdkafka and use_rdkafka else SimpleConsumer)
        return Cls(self,
                   self._cluster,
                   consumer_group=consumer_group,
                   **kwargs)

    def get_balanced_consumer(self, consumer_group, managed=False, **kwargs):
        """Return a BalancedConsumer of this topic

        :param consumer_group: The name of the consumer group to join
        :type consumer_group: bytes
        :param managed: If True, manage the consumer group with Kafka using the 0.9
            group management api (requires Kafka >=0.9))
        :type managed: bool
        """
        if not managed:
            if "zookeeper_connect" not in kwargs and \
                    self._cluster._zookeeper_connect is not None:
                kwargs['zookeeper_connect'] = self._cluster._zookeeper_connect
            cls = BalancedConsumer
        else:
            cls = ManagedBalancedConsumer
        return cls(self, self._cluster, consumer_group, **kwargs)
