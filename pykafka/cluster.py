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
import json
import logging
import random
import time
import weakref

from kazoo.client import KazooClient

from .broker import Broker
from .exceptions import (ERROR_CODES,
                         GroupCoordinatorNotAvailable,
                         NoBrokersAvailableError,
                         SocketDisconnectedError,
                         LeaderNotAvailable)
from .protocol import GroupCoordinatorRequest, GroupCoordinatorResponse
from .topic import Topic
from .utils.compat import iteritems, itervalues, range

log = logging.getLogger(__name__)


class TopicDict(dict):
    """Lazy dict, which will also attempt to auto-create unknown topics"""

    def __init__(self, cluster, exclude_internal_topics, *args, **kwargs):
        super(TopicDict, self).__init__(*args, **kwargs)
        self._cluster = weakref.ref(cluster)
        self._exclude_internal_topics = exclude_internal_topics

    def values(self):
        return [self[key] for key in self]

    def __getitem__(self, key):
        if not isinstance(key, bytes):
            raise TypeError("TopicDict.__getitem__ accepts a bytes object, but it "
                            "got '%s'", type(key))
        if self._should_exclude_topic(key):
            raise KeyError("You have configured KafkaClient/Cluster to hide "
                           "double-underscored, internal topics")
        topic_ref = super(TopicDict, self).__getitem__(key)
        if topic_ref is not None and topic_ref() is not None:
            return topic_ref()
        else:
            # Topic exists, but needs to be instantiated locally
            for i in range(self._cluster()._max_connection_retries):
                meta = self._cluster()._get_metadata([key])
                try:
                    topic = Topic(self._cluster(), meta.topics[key])
                except LeaderNotAvailable:
                    log.warning("LeaderNotAvailable encountered during Topic creation")
                    if i == self._cluster()._max_connection_retries - 1:
                        raise
                else:
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
                 source_address='',
                 zookeeper_hosts=None,
                 ssl_config=None):
        """Create a new Cluster instance.

        :param hosts: Comma-separated list of kafka hosts to which to connect.
        :type hosts: bytes
        :param zookeeper_hosts: KazooClient-formatted string of ZooKeeper hosts to which
            to connect. If not `None`, this argument takes precedence over `hosts`
        :type zookeeper_hosts: bytes
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
        :param ssl_config: Config object for SSL connection
        :type ssl_config: :class:`pykafka.connection.SslConfig`
        """
        self._seed_hosts = zookeeper_hosts if zookeeper_hosts is not None else hosts
        self._socket_timeout_ms = socket_timeout_ms
        self._offsets_channel_socket_timeout_ms = offsets_channel_socket_timeout_ms
        self._handler = handler
        self._brokers = {}
        self._topics = TopicDict(self, exclude_internal_topics)
        self._source_address = source_address
        self._source_host = self._source_address.split(':')[0]
        self._source_port = 0
        self._ssl_config = ssl_config
        self._zookeeper_connect = zookeeper_hosts
        self._max_connection_retries = 3
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

    def _request_metadata(self, broker_connects, topics):
        """Request broker metadata from a set of brokers

        Returns the result of the first successful metadata request

        :param broker_connects: The set of brokers to which to attempt to connect
        :type broker_connects: Iterable of two-element sequences of the format
            (broker_host, broker_port)
        """
        for i in range(self._max_connection_retries):
            for host, port in broker_connects:
                try:
                    broker = Broker(-1, host, int(port), self._handler,
                                    self._socket_timeout_ms,
                                    self._offsets_channel_socket_timeout_ms,
                                    buffer_size=1024 * 1024,
                                    source_host=self._source_host,
                                    source_port=self._source_port,
                                    ssl_config=self._ssl_config)
                    response = broker.request_metadata(topics)
                    if response is not None:
                        return response
                except Exception as e:
                    log.error('Unable to connect to broker %s:%s. Continuing.', host, port)
                    log.exception(e)

    def _get_metadata(self, topics=None):
        """Get fresh cluster metadata from a broker."""
        # Works either on existing brokers or seed_hosts list
        brokers = random.shuffle([b for b in self.brokers.values() if b.connected])
        if brokers:
            for broker in brokers:
                response = broker.request_metadata(topics)
                if response is not None:
                    return response
        else:  # try seed hosts
            if self._zookeeper_connect is not None:
                broker_connects = self._get_brokers_from_zookeeper(
                    self._zookeeper_connect)
                metadata = self._request_metadata(broker_connects, topics)
                if metadata is not None:
                    return metadata
            else:
                broker_connects = [
                    [broker_str.split(":")[0], broker_str.split(":")[1].split("/")[0]]
                    for broker_str in self._seed_hosts.split(',')]
                metadata = self._request_metadata(broker_connects, topics)
                if metadata is not None:
                    return metadata

        # Couldn't connect anywhere. Raise an error.
        raise NoBrokersAvailableError(
            'Unable to connect to a broker to fetch metadata. See logs.')

    def _get_brokers_from_zookeeper(self, zk_connect):
        """Build a list of broker connection pairs from a ZooKeeper host

        :param zk_connect: The ZooKeeper connect string of the instance to which to
            connect
        :type zk_connect: str
        """
        zookeeper = KazooClient(zk_connect, timeout=self._socket_timeout_ms / 1000)
        try:
            # This math is necessary due to a nested timeout in KazooClient.
            # KazooClient will attempt to retry its connections only until the
            # start() timeout is reached. Each of those retries will timeout as
            # indicated by the KazooClient kwarg. We do a number of timeouts of
            # self._socket_timeout_ms equal to the number of hosts. This provides
            # the same retrying behavior that pykafka uses above when treating
            # this host string as a list of kafka brokers.
            timeout = (len(zk_connect.split(',')) * self._max_connection_retries *
                       self._socket_timeout_ms) / 1000
            zookeeper.start(timeout=timeout)
        except Exception as e:
            log.error('Unable to connect to ZooKeeper instance %s', zk_connect)
            log.exception(e)
            return []
        else:
            try:
                # get a list of connect strings from zookeeper
                brokers_path = "/brokers/ids/"
                broker_ids = zookeeper.get_children(brokers_path)
                broker_connects = []
                for broker_id in broker_ids:
                    broker_json, _ = zookeeper.get("{}{}".format(brokers_path,
                                                                 broker_id))
                    broker_info = json.loads(broker_json.decode("utf-8"))
                    broker_connects.append((broker_info['host'],
                                            broker_info['port']))
                zookeeper.stop()
                self._zookeeper_connect = zk_connect
                return broker_connects
            except Exception as e:
                log.error('Unable to fetch broker info from ZooKeeper')
                log.exception(e)
                return []

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
                    source_port=self._source_port,
                    ssl_config=self._ssl_config)
            elif not self._brokers[id_].connected:
                log.info('Reconnecting to broker id %s: %s:%s', id_, meta.host, meta.port)
                import socket
                try:
                    self._brokers[id_].connect()
                except socket.error:
                    log.info('Failed to re-establish connection with broker id %s: %s:%s',
                             id_, meta.host, meta.port)
            else:
                broker = self._brokers[id_]
                if meta.host == broker.host and meta.port == broker.port:
                    log.info('Broker %s:%s metadata unchanged. Continuing.',
                             broker.host, broker.port)
                    continue  # no changes
                # TODO: Can brokers update? Seems like a problem if so.
                #       Figure out and implement update/disconnect/reconnect if
                #       needed.
                raise Exception('Broker host/port change detected! %s', broker)

    def get_group_coordinator(self, consumer_group):
        """Get the broker designated as the group coordinator for this consumer group.

        Based on Step 1 at https://cwiki.apache.org/confluence/display/KAFKA/Committing+and+fetching+consumer+offsets+in+Kafka

        :param consumer_group: The name of the consumer group for which to
            find the offset manager.
        :type consumer_group: str
        """
        log.info("Attempting to discover offset manager for consumer group '%s'",
                 consumer_group)
        for i in range(self._max_connection_retries):
            for broker in itervalues(self.brokers):
                if i > 0:
                    log.debug("Retrying offset manager discovery")
                time.sleep(i * 2)

                req = GroupCoordinatorRequest(consumer_group)
                future = broker.handler.request(req)
                try:
                    res = future.get(GroupCoordinatorResponse)
                except GroupCoordinatorNotAvailable:
                    log.error('Error discovering offset manager.')
                    if i == self._max_connection_retries - 1:
                        raise
                except SocketDisconnectedError:
                    log.error("Socket disconnected during offset manager "
                              "discovery. This can happen when using PyKafka "
                              "with a Kafka version lower than 0.8.2.")
                    if i == self._max_connection_retries - 1:
                        raise
                    self.update()
                else:
                    coordinator = self.brokers.get(res.coordinator_id, None)
                    if coordinator is None:
                        raise Exception('Coordinator broker with id {id_} not found'.format(id_=res.coordinator_id))
                    log.info("Found coordinator broker with id %s", res.coordinator_id)
                    return coordinator

    def update(self):
        """Update known brokers and topics."""
        for i in range(self._max_connection_retries):
            log.debug("Updating cluster, attempt {}/{}".format(i+1, self._max_connection_retries))
            metadata = self._get_metadata()
            if len(metadata.brokers) == 0 and len(metadata.topics) == 0:
                log.warning('No broker metadata found. If this is a fresh cluster, '
                            'this may be due to a bug in Kafka. You can force '
                            'broker metadata to be returned by manually creating '
                            'a topic in the cluster. See '
                            'https://issues.apache.org/jira/browse/KAFKA-2154 '
                            'for information.')
            self._update_brokers(metadata.brokers)
            try:
                self._topics._update_topics(metadata.topics)
            except LeaderNotAvailable:
                log.warning("LeaderNotAvailable encountered. This may be "
                            "because one or more partitions have no available replicas.")
            else:
                break
