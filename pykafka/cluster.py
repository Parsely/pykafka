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
from .exceptions import (ERROR_CODES,
                         ConsumerCoordinatorNotAvailable,
                         LeaderNotAvailable)
from .protocol import ConsumerMetadataRequest, ConsumerMetadataResponse
from .topic import Topic
from .utils.compat import iteritems, range

log = logging.getLogger(__name__)


class TopicDict(dict):
    """Lazy dict, which will also attempt to auto-create unknown topics"""

    def __init__(self, cluster, exclude_internal_topics, *args, **kwargs):
        super(TopicDict, self).__init__(*args, **kwargs)
        self._cluster = weakref.ref(cluster)
        self._exclude_internal_topics = exclude_internal_topics

    def __getitem__(self, key):
        topic_ref = super(TopicDict, self).__getitem__(key)
        if topic_ref is not None and topic_ref() is not None:
            return topic_ref()
        else:
            # Topic exists, but needs to be instantiated locally
            meta = self._cluster()._get_metadata([key])
            topic = Topic(self._cluster(), meta.topics[key])
            self[key] = weakref.ref(topic)
            return topic

    def __missing__(self, key):
        log.warning('Topic %s not found. Attempting to auto-create.', key)
        self._create_topic(key)

        # Note that __missing__ is called from within dict.__getitem__, so
        # that's what we should be returning (rather than self.__getitem__)
        return super(TopicDict, self).__getitem__(key)

    def _create_topic(self, topic_name):
        """Auto-create a topic.

        Not exposed in the cluster or broker because this is *only*
        auto-creation.  When there's a real API for creating topics,
        with settings and everything, we'll implement that. To expose just
        this now would be disingenuous, since it's features would be hobbled.
        """
        while True:
            # Auto-creating is as simple as issuing a metadata request
            # solely for that topic.  If topic auto-creation is enabled on the
            # broker, the initial response will carry a LeaderNotAvailable
            # error, otherwise it will be an UnknownTopicOrPartition or
            # possibly a RequestTimedOut
            res = self._cluster()._get_metadata(topics=[topic_name])
            err = res.topics[topic_name].err
            if err == LeaderNotAvailable.ERROR_CODE:
                time.sleep(.1)
            elif err == 0:
                log.info('Topic %s successfully auto-created.', topic_name)
                self._cluster().update()
                break
            else:
                raise ERROR_CODES[err](
                    "Failed to auto-create topic '{}'".format(topic_name))

    def _update_topics(self, metadata):
        """Update topics with fresh metadata.

        :param metadata: Metadata for all topics.
        :type metadata: Dict of `{name, metadata}` where `metadata` is
            :class:`pykafka.protocol.TopicMetadata` and `name` is `bytes`.
        """
        # Remove old topics
        removed = set(self.keys()) - set(metadata.keys())
        if len(removed) > 0:
            log.info("Removing %d topics", len(removed))
        for name in removed:
            log.debug("Removing topic '%s'", name)
            super(TopicDict, self).pop(name)

        # Add/update partition information
        if len(metadata) > 0:
            log.info("Discovered %d topics", len(metadata))
        for name, meta in iteritems(metadata):
            if not self._should_exclude_topic(name):
                if name not in self.keys():
                    self[name] = None  # to be instantiated lazily
                    log.debug("Discovered topic '%s'", name)
                else:
                    # avoid instantiating Topic if it isn't already there
                    ref = super(TopicDict, self).__getitem__(name)
                    if ref is not None and ref() is not None:
                        self[name].update(meta)

    def _should_exclude_topic(self, topic_name):
        """Should this topic be excluded from the list shown to the client?"""
        if not self._exclude_internal_topics:
            return False
        return topic_name.startswith(b"__")


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
                 exclude_internal_topics=True,
                 source_address=''):
        """Create a new Cluster instance.

        :param hosts: Comma-separated list of kafka hosts to used to connect.
        :type hosts: bytes
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
        :param source_address: The source address for socket connections
        :type source_address: str `'host:port'`
        """
        self._seed_hosts = hosts
        self._socket_timeout_ms = socket_timeout_ms
        self._offsets_channel_socket_timeout_ms = offsets_channel_socket_timeout_ms
        self._handler = handler
        self._brokers = {}
        self._topics = TopicDict(self, exclude_internal_topics)
        self._source_address = source_address
        self._source_host = self._source_address.split(':')[0]
        self._source_port = 0
        if ':' in self._source_address:
            self._source_port = int(self._source_address.split(':')[1])
        self.update()

    def __repr__(self):
        return "<{module}.{name} at {id_} (hosts={hosts})>".format(
            module=self.__class__.__module__,
            name=self.__class__.__name__,
            id_=hex(id(self)),
            hosts=self._seed_hosts,
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

    def _get_metadata(self, topics=None):
        """Get fresh cluster metadata from a broker."""
        # Works either on existing brokers or seed_hosts list
        brokers = [b for b in self.brokers.values() if b.connected]
        if brokers:
            for broker in brokers:
                response = broker.request_metadata(topics)
                if response is not None:
                    return response
        else:  # try seed hosts
            brokers = self._seed_hosts.split(',')
            for broker_str in brokers:
                try:
                    h, p = broker_str.split(':')
                    broker = Broker(-1, h, int(p), self._handler,
                                    self._socket_timeout_ms,
                                    self._offsets_channel_socket_timeout_ms,
                                    buffer_size=1024 * 1024,
                                    source_host=self._source_host,
                                    source_port=self._source_port)
                    response = broker.request_metadata(topics)
                    if response is not None:
                        return response
                except Exception as e:
                    log.error('Unable to connect to broker %s', broker_str)
                    log.exception(e)
        # Couldn't connect anywhere. Raise an error.
        raise RuntimeError(
            'Unable to connect to a broker to fetch metadata. See logs.')

    def _update_brokers(self, broker_metadata):
        """Update brokers with fresh metadata.

        :param broker_metadata: Metadata for all brokers.
        :type broker_metadata: Dict of `{name: metadata}` where `metadata` is
            :class:`pykafka.protocol.BrokerMetadata` and `name` is str.
        """
        # FIXME: A cluster with no topics returns no brokers in metadata
        # Remove old brokers
        removed = set(self._brokers.keys()) - set(broker_metadata.keys())
        if len(removed) > 0:
            log.info('Removing %d brokers', len(removed))
        for id_ in removed:
            log.debug('Removing broker %s', self._brokers[id_])
            self._brokers.pop(id_)
        # Add/update current brokers
        if len(broker_metadata) > 0:
            log.info('Discovered %d brokers', len(broker_metadata))
        for id_, meta in iteritems(broker_metadata):
            if id_ not in self._brokers:
                log.debug('Discovered broker id %s: %s:%s', id_, meta.host, meta.port)
                self._brokers[id_] = Broker.from_metadata(
                    meta, self._handler, self._socket_timeout_ms,
                    self._offsets_channel_socket_timeout_ms,
                    buffer_size=1024 * 1024,
                    source_host=self._source_host,
                    source_port=self._source_port
                )
            else:
                broker = self._brokers[id_]
                if meta.host == broker.host and meta.port == broker.port:
                    continue  # no changes
                # TODO: Can brokers update? Seems like a problem if so.
                #       Figure out and implement update/disconnect/reconnect if
                #       needed.
                raise Exception('Broker host/port change detected! %s', broker)

    def get_offset_manager(self, consumer_group):
        """Get the broker designated as the offset manager for this consumer group.

        Based on Step 1 at https://cwiki.apache.org/confluence/display/KAFKA/Committing+and+fetching+consumer+offsets+in+Kafka

        :param consumer_group: The name of the consumer group for which to
            find the offset manager.
        :type consumer_group: str
        """
        log.info("Attempting to discover offset manager for consumer group '%s'",
                 consumer_group)
        # arbitrarily choose a broker, since this request can go to any
        broker = self.brokers[random.choice(list(self.brokers.keys()))]
        MAX_RETRIES = 5

        for i in range(MAX_RETRIES):
            if i > 0:
                log.debug("Retrying offset manager discovery")
            time.sleep(i * 2)

            req = ConsumerMetadataRequest(consumer_group)
            future = broker.handler.request(req)
            try:
                res = future.get(ConsumerMetadataResponse)
            except ConsumerCoordinatorNotAvailable:
                log.error('Error discovering offset manager.')
                if i == MAX_RETRIES - 1:
                    raise
            else:
                coordinator = self.brokers.get(res.coordinator_id, None)
                if coordinator is None:
                    raise Exception('Coordinator broker with id {id_} not found'.format(id_=res.coordinator_id))
                log.info("Found coordinator broker with id %s", res.coordinator_id)
                return coordinator

    def update(self):
        """Update known brokers and topics."""
        max_retries = 3
        for i in range(max_retries):
            metadata = self._get_metadata()
            if len(metadata.brokers) == 0 and len(metadata.topics) == 0:
                log.warning('No broker metadata found. If this is a fresh cluster, '
                            'this may be due to a bug in Kafka. You can force '
                            'broker metadata to be returned by manually creating '
                            'a topic in the cluster. See '
                            'https://issues.apache.org/jira/browse/KAFKA-2154 '
                            'for information. Please note: topic auto-creation '
                            'will NOT work. You need to create at least one topic '
                            'manually using the Kafka CLI tools.')
            self._update_brokers(metadata.brokers)
            try:
                self._topics._update_topics(metadata.topics)
            except LeaderNotAvailable:
                log.warning("LeaderNotAvailable encountered. This is "
                            "because one or more partitions have no available replicas.")
            else:
                break
