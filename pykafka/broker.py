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
__all__ = ["Broker"]
import logging
import time

from .connection import BrokerConnection
from .exceptions import LeaderNotAvailable, SocketDisconnectedError
from .handlers import RequestHandler
from .protocol import (
    FetchRequest, FetchResponse, OffsetRequest,
    OffsetResponse, MetadataRequest, MetadataResponse,
    OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse,
    ProduceResponse)
from .utils.compat import range, iteritems

log = logging.getLogger(__name__)


class Broker():
    """
    A Broker is an abstraction over a real kafka server instance.
    It is used to perform requests to these servers.
    """
    def __init__(self,
                 id_,
                 host,
                 port,
                 handler,
                 socket_timeout_ms,
                 offsets_channel_socket_timeout_ms,
                 buffer_size=1024 * 1024,
                 source_host='',
                 source_port=0):
        """Create a Broker instance.

        :param id_: The id number of this broker
        :type id_: int
        :param host: The host address to which to connect. An IP address or a
            DNS name
        :type host: str
        :param port: The port on which to connect
        :type port: int
        :param handler: A Handler instance that will be used to service requests
            and responses
        :type handler: :class:`pykafka.handlers.Handler`
        :param socket_timeout_ms: The socket timeout for network requests
        :type socket_timeout_ms: int
        :param offsets_channel_socket_timeout_ms: The socket timeout for network
            requests on the offsets channel
        :type offsets_channel_socket_timeout_ms: int
        :param buffer_size: The size (bytes) of the internal buffer used to
            receive network responses
        :type buffer_size: int
        :param source_host: The host portion of the source address for
            socket connections
        :type source_host: str
        :param source_port: The port portion of the source address for
            socket connections
        :type source_port: int
        """
        self._connection = None
        self._offsets_channel_connection = None
        self._id = int(id_)
        self._host = host
        self._port = port
        self._source_host = source_host
        self._source_port = source_port
        self._handler = handler
        self._req_handler = None
        self._offsets_channel_req_handler = None
        self._socket_timeout_ms = socket_timeout_ms
        self._offsets_channel_socket_timeout_ms = offsets_channel_socket_timeout_ms
        self._buffer_size = buffer_size
        self.connect()

    def __repr__(self):
        return "<{module}.{name} at {id_} (host={host}, port={port}, id={my_id})>".format(
            module=self.__class__.__module__,
            name=self.__class__.__name__,
            id_=hex(id(self)),
            host=self._host,
            port=self._port,
            my_id=self._id
        )

    @classmethod
    def from_metadata(cls,
                      metadata,
                      handler,
                      socket_timeout_ms,
                      offsets_channel_socket_timeout_ms,
                      buffer_size=64 * 1024,
                      source_host='',
                      source_port=0):
        """Create a Broker using BrokerMetadata

        :param metadata: Metadata that describes the broker.
        :type metadata: :class:`pykafka.protocol.BrokerMetadata.`
        :param handler: A Handler instance that will be used to service requests
            and responses
        :type handler: :class:`pykafka.handlers.Handler`
        :param socket_timeout_ms: The socket timeout for network requests
        :type socket_timeout_ms: int
        :param offsets_channel_socket_timeout_ms: The socket timeout for network
            requests on the offsets channel
        :type offsets_channel_socket_timeout_ms: int
        :param buffer_size: The size (bytes) of the internal buffer used to
            receive network responses
        :type buffer_size: int
        :param source_host: The host portion of the source address for
            socket connections
        :type source_host: str
        :param source_port: The port portion of the source address for
            socket connections
        :type source_port: int
        """
        return cls(metadata.id, metadata.host,
                   metadata.port, handler, socket_timeout_ms,
                   offsets_channel_socket_timeout_ms,
                   buffer_size=buffer_size,
                   source_host=source_host,
                   source_port=source_port)

    @property
    def connected(self):
        """Returns True if this object's main connection to the Kafka broker
            is active
        """
        return self._connection.connected

    @property
    def offsets_channel_connected(self):
        """Returns True if this object's offsets channel connection to the
            Kafka broker is active
        """
        if self._offsets_channel_connection:
            return self._offsets_channel_connection.connected
        return False

    @property
    def id(self):
        """The broker's ID within the Kafka cluster"""
        return self._id

    @property
    def host(self):
        """The host to which this broker is connected"""
        return self._host

    @property
    def port(self):
        """The port where the broker is available"""
        return self._port

    @property
    def handler(self):
        """The primary :class:`pykafka.handlers.RequestHandler` for this broker

        This handler handles all requests outside of the commit/fetch api
        """
        return self._req_handler

    @property
    def offsets_channel_handler(self):
        """The offset channel :class:`pykafka.handlers.RequestHandler` for this
            broker

        This handler handles all requests that use the commit/fetch api
        """
        return self._offsets_channel_req_handler

    def connect(self):
        """Establish a connection to the broker server.

        Creates a new :class:`pykafka.connection.BrokerConnection` and a new
        :class:`pykafka.handlers.RequestHandler` for this broker
        """
        self._connection = BrokerConnection(self.host, self.port,
                                            buffer_size=self._buffer_size,
                                            source_host=self._source_host,
                                            source_port=self._source_port)
        self._connection.connect(self._socket_timeout_ms)
        self._req_handler = RequestHandler(self._handler, self._connection)
        self._req_handler.start()

    def connect_offsets_channel(self):
        """Establish a connection to the Broker for the offsets channel

        Creates a new :class:`pykafka.connection.BrokerConnection` and a new
        :class:`pykafka.handlers.RequestHandler` for this broker's offsets
        channel
        """
        self._offsets_channel_connection = BrokerConnection(
            self.host, self.port, buffer_size=self._buffer_size,
            source_host=self._source_host, source_port=self._source_port)
        self._offsets_channel_connection.connect(self._offsets_channel_socket_timeout_ms)
        self._offsets_channel_req_handler = RequestHandler(
            self._handler, self._offsets_channel_connection
        )
        self._offsets_channel_req_handler.start()

    def fetch_messages(self,
                       partition_requests,
                       timeout=30000,
                       min_bytes=1):
        """Fetch messages from a set of partitions.

        :param partition_requests: Requests of messages to fetch.
        :type partition_requests: Iterable of
            :class:`pykafka.protocol.PartitionFetchRequest`
        :param timeout: the maximum amount of time (in milliseconds)
            the server will block before answering the fetch request if there
            isn't sufficient data to immediately satisfy min_bytes
        :type timeout: int
        :param min_bytes: the minimum amount of data (in bytes) the server
            should return. If insufficient data is available the request will
            block for up to `timeout` milliseconds.
        :type min_bytes: int
        """
        future = self._req_handler.request(FetchRequest(
            partition_requests=partition_requests,
            timeout=timeout,
            min_bytes=min_bytes,
        ))
        # XXX - this call returns even with less than min_bytes of messages?
        return future.get(FetchResponse)

    def produce_messages(self, produce_request):
        """Produce messages to a set of partitions.

        :param produce_request: a request object indicating the messages to
            produce
        :type produce_request: :class:`pykafka.protocol.ProduceRequest`
        """
        if produce_request.required_acks == 0:
            self._req_handler.request(produce_request, has_response=False)
        else:
            future = self._req_handler.request(produce_request)
            return future.get(ProduceResponse)

    def request_offset_limits(self, partition_requests):
        """Request offset information for a set of topic/partitions

        :param partition_requests: requests specifying the partitions for which
            to fetch offsets
        :type partition_requests: Iterable of
            :class:`pykafka.protocol.PartitionOffsetRequest`
        """
        future = self._req_handler.request(OffsetRequest(partition_requests))
        return future.get(OffsetResponse)

    def request_metadata(self, topics=None):
        """Request cluster metadata

        :param topics: The topic names for which to request metadata
        :type topics: Iterable of `bytes`
        """
        max_retries = 3
        for i in range(max_retries):
            if i > 0:
                log.debug("Retrying")
            time.sleep(i)

            try:
                future = self._req_handler.request(MetadataRequest(topics=topics))
                response = future.get(MetadataResponse)
            except SocketDisconnectedError:
                log.warning("Encountered SocketDisconnectedError while requesting "
                            "metadata from broker %s:%s. Continuing.",
                            self.host, self.port)
                continue

            for name, topic_metadata in iteritems(response.topics):
                if topic_metadata.err == LeaderNotAvailable.ERROR_CODE:
                    log.warning("Leader not available for topic '%s'.", name)
                for pid, partition_metadata in iteritems(topic_metadata.partitions):
                    if partition_metadata.err == LeaderNotAvailable.ERROR_CODE:
                        log.warning("Leader not available for topic '%s' partition %d.",
                                    name, pid)
            return response

    ######################
    #  Commit/Fetch API  #
    ######################

    def commit_consumer_group_offsets(self,
                                      consumer_group,
                                      consumer_group_generation_id,
                                      consumer_id,
                                      preqs):
        """Commit offsets to Kafka using the Offset Commit/Fetch API

        Commit the offsets of all messages consumed so far by this consumer group with the Offset Commit/Fetch API

        Based on Step 2 here https://cwiki.apache.org/confluence/display/KAFKA/Committing+and+fetching+consumer+offsets+in+Kafka

        :param consumer_group: the name of the consumer group for which to
            commit offsets
        :type consumer_group: str
        :param consumer_group_generation_id: The generation ID for this consumer
            group
        :type consumer_group_generation_id: int
        :param consumer_id: The identifier for this consumer group
        :type consumer_id: str
        :param preqs: Requests indicating the partitions for which offsets
            should be committed
        :type preqs: Iterable of :class:`pykafka.protocol.PartitionOffsetCommitRequest`
        """
        if not self.offsets_channel_connected:
            self.connect_offsets_channel()
        req = OffsetCommitRequest(consumer_group,
                                  consumer_group_generation_id,
                                  consumer_id,
                                  partition_requests=preqs)
        return self._offsets_channel_req_handler.request(req).get(OffsetCommitResponse)

    def fetch_consumer_group_offsets(self, consumer_group, preqs):
        """Fetch the offsets stored in Kafka with the Offset Commit/Fetch API

        Based on Step 2 here https://cwiki.apache.org/confluence/display/KAFKA/Committing+and+fetching+consumer+offsets+in+Kafka

        :param consumer_group: the name of the consumer group for which to
            fetch offsets
        :type consumer_group: str
        :param preqs: Requests indicating the partitions for which offsets
            should be fetched
        :type preqs: Iterable of :class:`pykafka.protocol.PartitionOffsetFetchRequest`
        """
        if not self.offsets_channel_connected:
            self.connect_offsets_channel()
        req = OffsetFetchRequest(consumer_group, partition_requests=preqs)
        return self._offsets_channel_req_handler.request(req).get(OffsetFetchResponse)
