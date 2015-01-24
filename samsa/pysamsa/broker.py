import logging

from samsa import abstract
from .connection import BrokerConnection
from .handlers import RequestHandler
from .protocol import (
    FetchRequest, FetchResponse, ProduceRequest, OffsetRequest,
    OffsetResponse, MetadataRequest, MetadataResponse
)
from .utils import compression


logger = logging.getLogger(__name__)


class Broker(abstract.Broker):

    def __init__(self, metadata, handler, timeout):
        """Init a Broker.

        :param metadata: Metadata that describes the broker.
        :type metadata: :class:`samsa.pysamsa.protocol.BrokerMetadata.`
        :param handler: TODO: Fill in
        :type handler: TODO: Fill in
        :param timeout: TODO: Fill in
        :type timeout: :class:int
        """
        self._connected = False
        self._id = int(metadata.id)
        self._host = metadata.host
        self._port = metadata.port
        self._handler = handler
        self._reqhandler = None
        self._timeout = timeout
        self.connect()

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
        """The :class:`samsa.handlers.RequestHandler` for this broker."""
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
        future = self.handler.request(FetchRequest(
            partition_requests=partition_requests,
            timeout=10000,
            min_bytes=1,
        ))
        return future.get(FetchResponse)

    def produce_messages(self,
                         partition_requests,
                         compression_type=compression.NONE,
                         required_acks=1,
                         timeout=10000):
        req = ProduceRequest(partition_requests=partition_requests,
                             compression_type=compression_type,
                             required_acks=required_acks,
                             timeout=timeout)
        self.handler.request(req).get()
        return None  # errors raised on deserialize, no need for return value

    def request_offsets(self, partition_requests):
        """Request offset information for a set of topic/partitions"""
        future = self.handler.request(OffsetRequest(partition_requests))
        return future.get(OffsetResponse)

    def request_metadata(self, topics=[]):
        future = self.handler.request(MetadataRequest(topics=topics))
        return future.get(MetadataResponse)
