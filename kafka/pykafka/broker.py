import logging

from kafka import base
from .connection import BrokerConnection
from .handlers import RequestHandler
from .protocol import (
    FetchRequest, FetchResponse, OffsetRequest,
    OffsetResponse, MetadataRequest, MetadataResponse
)


logger = logging.getLogger(__name__)


class Broker(base.BaseBroker):

    def __init__(self, id_, host, port, handler, timeout):
        """Init a Broker.

        :param handler: TODO: Fill in
        :type handler: TODO: Fill in
        :param timeout: TODO: Fill in
        :type timeout: :class:int
        """
        self._connected = False
        self._id = int(id_)
        self._host = host
        self._port = port
        self._handler = handler
        self._reqhandler = None
        self._timeout = timeout
        self.connect()

    @classmethod
    def from_metadata(cls, metadata, handler, timeout):
        """ Create a Broker using BrokerMetadata

        :param metadata: Metadata that describes the broker.
        :type metadata: :class:`kafka.pykafka.protocol.BrokerMetadata.`
        """
        return cls(metadata.id, metadata.host,
                   metadata.port, handler, timeout)

    @property
    def connected(self):
        """Returns True if the connected to the broker."""
        return self._connected

    @property
    def id(self):
        """The broker's ID within the Kafka cluster."""
        return self._id

    @property
    def host(self):
        """The host where the broker is available."""
        return self._host

    @property
    def port(self):
        """The port where the broker is available."""
        return self._port

    @property
    def handler(self):
        """The :class:`kafka.handlers.RequestHandler` for this broker."""
        return self._reqhandler

    def connect(self):
        """Establish a connection to the Broker."""
        conn = BrokerConnection(self.host, self.port)
        conn.connect(self._timeout)
        self._reqhandler = RequestHandler(self._handler, conn)
        self._reqhandler.start()
        self._connected = True

    def fetch_messages(self,
                       partition_requests,
                       timeout=30000,
                       min_bytes=1024):
        """Fetch messages from a set of partitions.

        :param partition_requests: Requests of messages to fetch.
        :type partition_requests: Iterable of
            :class:`kafka.pykafka.protocol.PartitionFetchRequest`
        """
        future = self.handler.request(FetchRequest(
            partition_requests=partition_requests,
            timeout=10000,
            min_bytes=1,
        ))
        return future.get(FetchResponse)

    def produce_messages(self, produce_request):
        """Produce messages to a set of partitions.

        :type partition_requests: Iterable of
            :class:`kafka.pykafka.protocol.ProduceRequest`
        """
        if produce_request.required_acks == 0:
            self.handler.request(produce_request, has_response=False)
        else:
            self.handler.request(produce_request).get()
            # Any errors will be decoded and raised in the `.get()`
        return None

    def request_offset_limits(self, partition_requests):
        """Request offset information for a set of topic/partitions"""
        future = self.handler.request(OffsetRequest(partition_requests))
        return future.get(OffsetResponse)

    def commit_offsets(self, consumer_group, partition_requests):
        """Use the Offset Commit/Fetch API to commit offsets for a consumer
            group
        """
        pass

    def request_metadata(self, topics=[]):
        future = self.handler.request(MetadataRequest(topics=topics))
        return future.get(MetadataResponse)
