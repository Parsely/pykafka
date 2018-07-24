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
    FetchRequest, FetchResponse, ListOffsetRequest, ListOffsetResponse, MetadataRequest,
    MetadataResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, ProduceResponse, JoinGroupRequest, JoinGroupResponse,
    SyncGroupRequest, SyncGroupResponse, HeartbeatRequest, HeartbeatResponse,
    LeaveGroupRequest, LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse,
    DescribeGroupsRequest, DescribeGroupsResponse, ApiVersionsRequest,
    ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest,
    DeleteTopicsResponse)
from .utils.compat import range, iteritems, get_bytes

log = logging.getLogger(__name__)


def _check_handler(fn):
    """Ensures that self._req_handler is not None before calling fn"""
    def wrapped(self, *args, **kwargs):
        if self._req_handler is None:
            raise SocketDisconnectedError
        return fn(self, *args, **kwargs)
    return wrapped


class Broker(object):
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
                 source_port=0,
                 ssl_config=None,
                 broker_version="0.9.0",
                 api_versions=None):
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
        :param ssl_config: Config object for SSL connection
        :type ssl_config: :class:`pykafka.connection.SslConfig`
        :param broker_version: The protocol version of the cluster being connected to.
            If this parameter doesn't match the actual broker version, some pykafka
            features may not work properly.
        :type broker_version: str
        :param api_versions: A sequence of :class:`pykafka.protocol.ApiVersionsSpec`
            objects indicating the API version compatibility of this broker
        :type api_versions: Iterable of :class:`pykafka.protocol.ApiVersionsSpec`
        """
        self._connection = None
        self._offsets_channel_connection = None
        self._id = int(id_)
        self._host = host
        self._port = port
        self._source_host = source_host
        self._source_port = source_port
        self._ssl_config = ssl_config
        self._handler = handler
        self._req_handler = None
        self._offsets_channel_req_handler = None
        self._socket_timeout_ms = socket_timeout_ms
        self._offsets_channel_socket_timeout_ms = offsets_channel_socket_timeout_ms
        self._buffer_size = buffer_size
        self._req_handlers = {}
        self._broker_version = broker_version
        self._api_versions = api_versions
        try:
            self.connect()
        except SocketDisconnectedError:
            log.warning("Failed to connect to broker at {host}:{port}. Check the "
                        "`listeners` property in server.config."
                        .format(host=self._host, port=self._port))

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
                      source_port=0,
                      ssl_config=None,
                      broker_version="0.9.0",
                      api_versions=None):
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
        :param ssl_config: Config object for SSL connection
        :type ssl_config: :class:`pykafka.connection.SslConfig`
        :param broker_version: The protocol version of the cluster being connected to.
            If this parameter doesn't match the actual broker version, some pykafka
            features may not work properly.
        :type broker_version: str
        :param api_versions: A sequence of :class:`pykafka.protocol.ApiVersionsSpec`
            objects indicating the API version compatibility of this broker
        :type api_versions: Iterable of :class:`pykafka.protocol.ApiVersionsSpec`
        """
        return cls(metadata.id, metadata.host,
                   metadata.port, handler, socket_timeout_ms,
                   offsets_channel_socket_timeout_ms,
                   buffer_size=buffer_size,
                   source_host=source_host,
                   source_port=source_port,
                   ssl_config=ssl_config,
                   broker_version=broker_version,
                   api_versions=api_versions)

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

    def connect(self, attempts=3):
        """Establish a connection to the broker server.

        Creates a new :class:`pykafka.connection.BrokerConnection` and a new
        :class:`pykafka.handlers.RequestHandler` for this broker
        """
        self._connection = BrokerConnection(self.host, self.port,
                                            self._handler,
                                            buffer_size=self._buffer_size,
                                            source_host=self._source_host,
                                            source_port=self._source_port,
                                            ssl_config=self._ssl_config)
        self._connection.connect(self._socket_timeout_ms, attempts=attempts)
        self._req_handler = RequestHandler(self._handler, self._connection)
        self._req_handler.start()

    def connect_offsets_channel(self, attempts=3):
        """Establish a connection to the Broker for the offsets channel

        Creates a new :class:`pykafka.connection.BrokerConnection` and a new
        :class:`pykafka.handlers.RequestHandler` for this broker's offsets
        channel
        """
        self._offsets_channel_connection = BrokerConnection(
            self.host, self.port, self._handler,
            buffer_size=self._buffer_size,
            source_host=self._source_host, source_port=self._source_port,
            ssl_config=self._ssl_config)
        self._offsets_channel_connection.connect(self._offsets_channel_socket_timeout_ms,
                                                 attempts=attempts)
        self._offsets_channel_req_handler = RequestHandler(
            self._handler, self._offsets_channel_connection
        )
        self._offsets_channel_req_handler.start()

    def _get_unique_req_handler(self, connection_id):
        """Return a RequestHandler instance unique to the given connection_id

        In some applications, for example the Group Membership API, requests running
        in the same process must be interleaved. When both of these requests are
        using the same RequestHandler instance, the requests are queued and the
        interleaving semantics are not upheld. This method behaves identically to
        self._req_handler if there is only one connection_id per KafkaClient.
        If a single KafkaClient needs to use more than one connection_id, this
        method maintains a dictionary of connections unique to those ids.

        :param connection_id: The unique identifier of the connection to return
        :type connection_id: str
        """
        if len(self._req_handlers) == 0:
            self._req_handlers[connection_id] = self._req_handler
        elif connection_id not in self._req_handlers:
            conn = BrokerConnection(
                self.host, self.port, self._handler, buffer_size=self._buffer_size,
                source_host=self._source_host, source_port=self._source_port)
            conn.connect(self._socket_timeout_ms)
            handler = RequestHandler(self._handler, conn)
            handler.start()
            self._req_handlers[connection_id] = handler
        return self._req_handlers[connection_id]

    @_check_handler
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
        request_class = FetchRequest.get_version_impl(self._api_versions)
        response_class = FetchResponse.get_version_impl(self._api_versions)
        future = self._req_handler.request(request_class(
            partition_requests=partition_requests,
            timeout=timeout,
            min_bytes=min_bytes,
            api_version=response_class.API_VERSION
        ))
        # XXX - this call returns even with less than min_bytes of messages?
        return future.get(response_class, broker_version=self._broker_version)

    @_check_handler
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

    @_check_handler
    def request_offset_limits(self, partition_requests):
        """Request offset information for a set of topic/partitions

        :param partition_requests: requests specifying the partitions for which
            to fetch offsets
        :type partition_requests: Iterable of
            :class:`pykafka.protocol.PartitionOffsetRequest`
        """
        request_class = ListOffsetRequest.get_version_impl(self._api_versions)
        response_class = ListOffsetResponse.get_version_impl(self._api_versions)
        future = self._req_handler.request(request_class(partition_requests))
        return future.get(response_class)

    @_check_handler
    def request_metadata(self, topics=None):
        """Request cluster metadata

        :param topics: The topic names for which to request metadata
        :type topics: Iterable of `bytes`
        """
        request_class = MetadataRequest.get_version_impl(self._api_versions)
        response_class = MetadataResponse.get_version_impl(self._api_versions)

        max_retries = 3
        for i in range(max_retries):
            if i > 0:
                log.debug("Retrying")
            time.sleep(i)

            try:
                future = self._req_handler.request(request_class(topics=topics))
                response = future.get(response_class)
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
                                  get_bytes(consumer_group_generation_id),
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
        request_class = OffsetFetchRequest.get_version_impl(self._api_versions)
        response_class = OffsetFetchResponse.get_version_impl(self._api_versions)
        req = request_class(consumer_group, partition_requests=preqs)
        return self._offsets_channel_req_handler.request(req).get(response_class)

    ##########################
    #  Group Membership API  #
    ##########################

    def join_group(self,
                   connection_id,
                   consumer_group,
                   member_id,
                   topic_name,
                   membership_protocol):
        """Send a JoinGroupRequest

        :param connection_id: The unique identifier of the connection on which to make
            this request
        :type connection_id: str
        :param consumer_group: The name of the consumer group to join
        :type consumer_group: bytes
        :param member_id: The ID of the consumer joining the group
        :type member_id: bytes
        :param topic_name: The name of the topic to which to connect, used in protocol
            metadata
        :type topic_name: str
        :param membership_protocol: The group membership protocol to which this request
            should adhere
        :type membership_protocol: :class:`pykafka.membershipprotocol.GroupMembershipProtocol`
        """
        handler = self._get_unique_req_handler(connection_id)
        if handler is None:
            raise SocketDisconnectedError
        future = handler.request(JoinGroupRequest(consumer_group, member_id, topic_name,
                                                  membership_protocol))
        self._handler.sleep()
        return future.get(JoinGroupResponse)

    def leave_group(self, connection_id, consumer_group, member_id):
        """Send a LeaveGroupRequest

        :param connection_id: The unique identifier of the connection on which to make
            this request
        :type connection_id: str
        :param consumer_group: The name of the consumer group to leave
        :type consumer_group: bytes
        :param member_id: The ID of the consumer leaving the group
        :type member_id: bytes
        """
        handler = self._get_unique_req_handler(connection_id)
        if handler is None:
            raise SocketDisconnectedError
        future = handler.request(LeaveGroupRequest(consumer_group, member_id))
        return future.get(LeaveGroupResponse)

    def sync_group(self, connection_id, consumer_group, generation_id, member_id, group_assignment):
        """Send a SyncGroupRequest

        :param connection_id: The unique identifier of the connection on which to make
            this request
        :type connection_id: str
        :param consumer_group: The name of the consumer group to which this consumer
            belongs
        :type consumer_group: bytes
        :param generation_id: The current generation for the consumer group
        :type generation_id: int
        :param member_id: The ID of the consumer syncing
        :type member_id: bytes
        :param group_assignment: A sequence of :class:`pykafka.protocol.MemberAssignment`
            instances indicating the partition assignments for each member of the group.
            When `sync_group` is called by a member other than the leader of the group,
            `group_assignment` should be an empty sequence.
        :type group_assignment: iterable of :class:`pykafka.protocol.MemberAssignment`
        """
        handler = self._get_unique_req_handler(connection_id)
        if handler is None:
            raise SocketDisconnectedError
        future = handler.request(
            SyncGroupRequest(consumer_group, generation_id, member_id, group_assignment))
        return future.get(SyncGroupResponse)

    def heartbeat(self, connection_id, consumer_group, generation_id, member_id):
        """Send a HeartbeatRequest

        :param connection_id: The unique identifier of the connection on which to make
            this request
        :type connection_id: str
        :param consumer_group: The name of the consumer group to which this consumer
            belongs
        :type consumer_group: bytes
        :param generation_id: The current generation for the consumer group
        :type generation_id: int
        :param member_id: The ID of the consumer sending this heartbeat
        :type member_id: bytes
        """
        handler = self._get_unique_req_handler(connection_id)
        if handler is None:
            raise SocketDisconnectedError
        future = handler.request(
            HeartbeatRequest(consumer_group, generation_id, member_id))
        self._handler.sleep()
        return future.get(HeartbeatResponse)

    ########################
    #  Administrative API  #
    ########################
    @_check_handler
    def list_groups(self):
        """Send a ListGroupsRequest"""
        future = self._req_handler.request(ListGroupsRequest())
        return future.get(ListGroupsResponse)

    @_check_handler
    def describe_groups(self, group_ids):
        """Send a DescribeGroupsRequest

        :param group_ids: A sequence of group identifiers for which to return descriptions
        :type group_ids: sequence of str
        """
        future = self._req_handler.request(DescribeGroupsRequest(group_ids))
        return future.get(DescribeGroupsResponse)

    ############################
    # Create/Delete Topics API #
    ############################
    @_check_handler
    def create_topics(self, topic_reqs, timeout=0):
        """Create topics via the Topic Creation API

        :param topic_reqs: The topic creation requests to issue
        :type topics: Iterable of :class:`pykafka.protocol.CreateTopicRequest`
        :param timeout: The time in ms to wait for a topic to be completely created.
            Values <= 0 will trigger topic creation and return immediately.
        :type timeout: int
        """
        future = self._req_handler.request(CreateTopicsRequest(topic_reqs,
                                                               timeout=timeout))
        return future.get(CreateTopicsResponse)

    @_check_handler
    def delete_topics(self, topics, timeout=0):
        """Delete topics viaa the Topic Deletion API

        :param topics: The names of the topics to delete
        :type topics: Iterable of str
        :param timeout: The time in ms to wait for a topic to be completely deleted.
            Values <= 0 will trigger topic deletion and return immediately.
        :type timeout: int
        """
        future = self._req_handler.request(DeleteTopicsRequest(topics, timeout=timeout))
        return future.get(DeleteTopicsResponse)

    @_check_handler
    def fetch_api_versions(self):
        """Fetch supported API versions from this broker"""
        future = self._req_handler.request(ApiVersionsRequest())
        return future.get(ApiVersionsResponse.get_version_impl(self._api_versions))
