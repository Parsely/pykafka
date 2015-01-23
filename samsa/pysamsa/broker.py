from samsa import abstract

class Broker(abstract.Broker):
    """A Kafka broker.

    :param cluster: The cluster this broker is associated with.
    :type cluster: :class:`samsa.cluster.Cluster`
    :param id_: Kafka broker ID
    """
    def __init__(self, id_, host, port, handler, timeout):
        self._connected = False
        self._id = int(id_)
        self._host = host
        self._port = port
        self._handler = handler
        self._reqhandler = None
        self._timeout = timeout
        self.client = None

    __repr__ = attribute_repr('id', 'host', 'port')

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

    def fetch_messages(self, partition_requests, timeout=30000, min_bytes=1024):
        if not self.connected:
            self.connect()
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
        if not self.connected:
            self.connect()
        req = ProduceRequest(partition_requests=partition_requests,
                             compression_type=compression_type,
                             required_acks=required_acks,
                             timeout=timeout)
        future = self.handler.request(req)
        res = future.get(ProduceResponse)
        return None # errors raised on deserialize, no need for return value

    def request_offsets(self, partition_requests):
        """Request offset information for a set of topic/partitions"""
        if not self.connected:
            self.connect()
        future = self.handler.request(OffsetRequest(partition_requests))
        return future.get(OffsetResponse)

    def request_metadata(self, topics=[]):
        if not self.connected:
            self.connect()
        future = self.handler.request(MetadataRequest(topics=topics))
        return future.get(MetadataResponse)
