import abc


class BaseCluster(object):
    """Abstraction of a Kafka cluster.

    :ivar topics: Topics present in this cluster.
    :ivar brokers: Brokers in the cluster.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def brokers():
        pass

    @abc.abstractproperty
    def topics():
        pass

    @abc.abstractmethod
    def update():
        """Update the Cluster with metadata from Kafka.

        All updates must happen in-place. This means that if a Topic leader has
        changed, a new Topic can't be created and put into `self.topics`. That
        would break any clients that have instances of the old Topic. Instead,
        the current topic is updated seamlessly.
        """
        pass


class BaseBroker(object):
    __metaclass__ = abc.ABCMeta
    pass

    @abc.abstractproperty
    def id(self):
        pass

    @abc.abstractproperty
    def host(self):
        pass

    @abc.abstractproperty
    def port(self):
        pass


class BasePartition(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def id(self):
        pass

    @abc.abstractproperty
    def leader(self):
        pass

    @abc.abstractproperty
    def replicas(self):
        pass

    @abc.abstractproperty
    def isr(self):
        pass

    @abc.abstractproperty
    def topic(self):
        pass

    @abc.abstractmethod
    def latest_offset(self):
        pass

    @abc.abstractmethod
    def earliest_offset(self):
        pass


class BaseTopic(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def name(self):
        pass

    @abc.abstractproperty
    def partitions(self):
        pass

    @abc.abstractmethod
    def latest_offsets(self):
        """Get the latest offset for all partitions."""
        pass

    @abc.abstractmethod
    def earliest_offsets(self):
        """Get the earliest offset for all partitions."""
        pass


class BaseSimpleConsumer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, client, topic, partitions=None):
        """Create a consumer for a topic.

        :param client: Client connection to the cluster.
        :type client: :class:`kafka.client.KafkaClient`
        :param topic: The topic to consume from.
        :type topic: :class:`kafka.abstract.Topic` or :class:`str`
        :param partitions: List of partitions to consume from.
        :type partitions: Iterable of :class:`kafka.abstract.Partition` or int
        """
        pass

    @abc.abstractproperty
    def topic(self):
        pass

    @abc.abstractproperty
    def partitions(self):
        pass

    @abc.abstractmethod
    def __iter__(self):
        """Iterator for messages in the consumer."""
        pass

    @abc.abstractmethod
    def consume(self, timeout=None):
        """Consume a message from the topic."""
        pass


class BaseProducer(object):
    """Basic Producer for the Kafka Cluster.

    Synchronously publishes messages to Kafka as `produce` is called.
    """
    __metaclass__ = abc.ABCMeta

    @property
    def topic(self):
        return self._topic

    @property
    def partitioner(self):
        return self._partitioner

    @abc.abstractmethod
    def produce(self, messages):
        """Produce messages for the topic.

        :param messages: Iterable of messages to be published.
        :type messages: An iterable of either strings or (key, value) tuples.
            If tuples, then the `key` will be sent to the partitioner to
            determine to which partition it belongs. It will also be sent to
            Kafka and available when the message is read.
        """
        pass


class BaseAsyncProducer(BaseProducer):
    """Asynchronous Producer for the Kafka Cluster.

    Asynchronously publishes messages to the Kafka cluster. Calling `produce`
    will return immediately.  Messages will be batched and published at regular
    intervals based on settings passed to the AsyncProducer.
    """
    pass
