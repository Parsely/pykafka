from __future__ import division
"""
Author: Keith Bourgoin
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
__all__ = ["Cluster"]
import logging
import random
import time
import weakref

from .broker import Broker
from .exceptions import (ConsumerCoordinatorNotAvailable,
                         UnknownTopicOrPartition)
from .protocol import ConsumerMetadataRequest, ConsumerMetadataResponse
from .topic import Topic


logger = logging.getLogger(__name__)


class TopicDict(dict):
    """Dictionary which will attempt to auto-create unknown topics."""

    def __init__(self, cluster, *args, **kwargs):
        super(TopicDict, self).__init__(*args, **kwargs)
        self._cluster = weakref.proxy(cluster)

    def __missing__(self, key):
        logger.info('Topic %s not found. Attempting to auto-create.', key)
        if self._create_topic(key):
            return self[key]
        else:
            raise UnknownTopicOrPartition('Unknown topic: {}'.format(key))

    def _create_topic(self, topic_name):
        """Auto-create a topic.

        Not exposed in the cluster or broker because this is *only*
        auto-creation.  When there's a real API for creating topics,
        with settings and everything, we'll implement that. To expose just
        this now would be disingenuous, since it's features would be hobbled.
        """
        # Auto-creating will take a moment, so we try 5 times.
        for i in xrange(5):
            # Auto-creating is as simple as issuing a metadata request
            # solely for that topic.  The update is just to be sure
            # our `Cluster` knows about it.
            self._cluster.brokers[self._cluster.brokers.keys()[0]].request_metadata(topics=[topic_name])
            self._cluster.update()
            if topic_name in self:
                logger.info('Topic %s successfully auto-created.', topic_name)
                return True
            time.sleep(0.1)


class Cluster(object):
    """
    A Cluster is a high-level abstraction of the collection of brokers and
    topics that makes up a real kafka cluster.
    """
    def __init__(self,
                 hosts,
                 handler,
                 socket_timeout_ms=30 * 1000,
                 offsets_channel_socket_timeout_ms=10 * 1000,
                 exclude_internal_topics=True):
        """Create a new Cluster instance.

        :param hosts: Comma-separated list of kafka hosts to used to connect.
        :type hosts: str
        :param handler: The concurrency handler for network requests.
        :type handler: :class:`pykafka.handlers.Handler`
        :param socket_timeout_ms: The socket timeout (in milliseconds) for
            network requests
        :type socket_timeout_ms: int
        :param offsets_channel_socket_timeout_ms: The socket timeout (in
            milliseconds) when reading responses for offset commit and
            offset fetch requests.
        :type offsets_channel_socket_timeout_ms: int
        :param exclude_internal_topics: Whether messages from internal topics
            (specifically, the offsets topic) should be exposed to consumers.
        :type exclude_internal_topics: bool
        """
        self._seed_hosts = hosts
        self._socket_timeout_ms = socket_timeout_ms
        self._offsets_channel_socket_timeout_ms = offsets_channel_socket_timeout_ms
        self._handler = handler
        self._brokers = {}
        self._topics = TopicDict(self)
        self._exclude_internal_topics = exclude_internal_topics
        self.update()

    def __repr__(self):
        return "<{}.{} at {} (hosts={})>".format(
            self.__class__.__module__,
            self.__class__.__name__,
            hex(id(self)),
            self._seed_hosts,
        )

    @property
    def brokers(self):
        """The dict of known brokers for this cluster"""
        return self._brokers

    @property
    def topics(self):
        """The dict of known topics for this cluster"""
        return self._topics

    @property
    def handler(self):
        """The concurrency handler for network requests"""
        return self._handler

    def _get_metadata(self):
        """Get fresh cluster metadata from a broker."""
        # Works either on existing brokers or seed_hosts list
        brokers = [b for b in self.brokers.values() if b.connected]
        if brokers:
            for broker in brokers:
                response = broker.request_metadata()
                if response is not None:
                    return response
        else:  # try seed hosts
            brokers = self._seed_hosts.split(',')
            for broker_str in brokers:
                try:
                    h, p = broker_str.split(':')
                    broker = Broker(-1, h, p, self._handler,
                                    self._socket_timeout_ms,
                                    self._offsets_channel_socket_timeout_ms,
                                    buffer_size=1024 * 1024)
                    response = broker.request_metadata()
                    if response is not None:
                        return response
                except:
                    logger.exception('Unable to connect to broker %s',
                                     broker_str)
        # Couldn't connect anywhere. Raise an error.
        raise Exception('Unable to connect to a broker to fetch metadata.')

    def _update_brokers(self, broker_metadata):
        """Update brokers with fresh metadata.

        :param broker_metadata: Metadata for all brokers.
        :type broker_metadata: Dict of `{name: metadata}` where `metadata` is
            :class:`pykafka.protocol.BrokerMetadata` and `name` is str.
        """
        # FIXME: A cluster with no topics returns no brokers in metadata
        # Remove old brokers
        removed = set(self._brokers.keys()) - set(broker_metadata.keys())
        for id_ in removed:
            logger.info('Removing broker %s', self._brokers[id_])
            self._brokers.pop(id_)
        # Add/update current brokers
        for id_, meta in broker_metadata.iteritems():
            if id_ not in self._brokers:
                logger.info('Discovered broker id %s: %s:%s', id_, meta.host, meta.port)
                self._brokers[id_] = Broker.from_metadata(
                    meta, self._handler, self._socket_timeout_ms,
                    self._offsets_channel_socket_timeout_ms,
                    buffer_size=1024 * 1024
                )
            else:
                broker = self._brokers[id_]
                if meta.host == broker.host and meta.port == broker.port:
                    continue  # no changes
                # TODO: Can brokers update? Seems like a problem if so.
                #       Figure out and implement update/disconnect/reconnect if
                #       needed.
                raise Exception('Broker host/port change detected! %s', broker)

    def _update_topics(self, metadata):
        """Update topics with fresh metadata.

        :param metadata: Metadata for all topics.
        :type metadata: Dict of `{name, metadata}` where `metadata` is
            :class:`pykafka.protocol.TopicMetadata` and `name` is str.
        """
        # Remove old topics
        removed = set(self._topics.keys()) - set(metadata.keys())
        for name in removed:
            logger.info('Removing topic %s', self._topics[name])
            self._topics.pop(name)
        # Add/update partition information
        for name, meta in metadata.iteritems():
            if not self._should_exclude_topic(name):
                if name not in self._topics:
                    self._topics[name] = Topic(self, meta)
                    logger.info('Discovered topic %s', self._topics[name])
                else:
                    self._topics[name].update(meta)

    def _should_exclude_topic(self, topic_name):
        """Should this topic be excluded from the list shown to the client?"""
        if not self._exclude_internal_topics:
            return False
        return topic_name.startswith("__")

    def get_offset_manager(self, consumer_group):
        """Get the broker designated as the offset manager for this consumer group.

        Based on Step 1 at https://cwiki.apache.org/confluence/display/KAFKA/Committing+and+fetching+consumer+offsets+in+Kafka

        :param consumer_group: The name of the consumer group for which to
            find the offset manager.
        :type consumer_group: str
        """
        logger.info("Attempting to discover offset manager for consumer group '%s'",
                    consumer_group)
        # arbitrarily choose a broker, since this request can go to any
        broker = self.brokers[random.choice(self.brokers.keys())]
        MAX_RETRIES = 3

        for i in xrange(MAX_RETRIES):
            if i > 0:
                logger.info("Retrying")
            time.sleep(i)

            req = ConsumerMetadataRequest(consumer_group)
            future = broker.handler.request(req)
            try:
                res = future.get(ConsumerMetadataResponse)
            except ConsumerCoordinatorNotAvailable:
                logger.error('Error discovering offset manager.')
                if i == MAX_RETRIES - 1:
                    raise
            else:
                coordinator = self.brokers.get(res.coordinator_id, None)
                if coordinator is None:
                    raise Exception('Coordinator broker with id {} not found'.format(res.coordinator_id))
                logger.info("Found coordinator broker with id %s", res.coordinator_id)
                return coordinator

    def update(self):
        """Update known brokers and topics."""
        metadata = self._get_metadata()
        if len(metadata.brokers) == 0 and len(metadata.topics) == 0:
            logger.warning('No broker metadata found. If this is a fresh cluster, '
                           'this may be due to a bug in Kafka. You can force '
                           'broker metadata to be returned by manually creating '
                           'a topic in the cluster. See '
                           'https://issues.apache.org/jira/browse/KAFKA-2154 '
                           'for information.')
        self._update_brokers(metadata.brokers)
        self._update_topics(metadata.topics)
