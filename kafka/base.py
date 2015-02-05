class BaseCluster(object):
    """A Kafka cluster.

    This is an abstraction of the cluster topology. It provides access
    to topics and brokers, which can be useful for introspection of a cluster.
    """

    @property
    def brokers(self):
        """Brokers associated with this cluster.

        :type: `dict` of {broker_id: :class:`kafka.base.BaseBroker`}
        """
        return self._brokers

    @property
    def topics(self):
        """Topics present in this cluster.

        :type: `dict` of {topic_name: :class:`kafka.base.BaseTopic`}
        """
        return self._topics

    def update(self):
        """Update the Cluster with metadata from Kafka.

        All updates must happen in-place. This means that if a Topic leader has
        changed, a new Topic can't be created and put into `self.topics`. That
        would break any clients that have instances of the old Topic. Instead,
        the current topic is updated seamlessly.
        """
        raise NotImplementedError


class BaseBroker(object):
    """A Kafka Broker.

    Not especially useful under normal circumstances, but can be handy
    when introspecting about a Cluster.
    """

    @property
    def id(self):
        """Id of the broker

        :type: `int`
        """
        return self._id

    @property
    def host(self):
        """Host of the broker.

        :type: `str`
        """
        return self._host

    @property
    def port(self):
        """Port the broker uses.

        :type: `int`
        """
        return self._port


class BasePartition(object):
    """A Kafka Partition.

    Each Kafka topic is split up into parts called "partitions".  When reading
    or writing a topic, you're actually reading a partition of the topic.
    Replication also happens at the partition level.

    Like Brokers, Partitions aren't useful under normal circumstances, but
    are handy to know about for debugging and introspection.
    """

    @property
    def id(self):
        """The id of this partition.

        :type: `int`
        """
        return self._id

    @property
    def leader(self):
        """The leader broker of the partition.

        :type: :class:`kafka.base.BasePartition`
        """
        return self._leader

    @property
    def replicas(self):
        """List of brokers which has replicas of this Partition.

        :type: `list` of :class:`kafka.base.BaseBroker`
        """
        return self._replicas

    @property
    def isr(self):
        """List of brokers which have in-sync replicas of this partition.

        :type: `list` of :class:`kafka.base.BaseBroker`
        """
        return self._isr

    @property
    def topic(self):
        """Name of the topic to which this partition belongs.

        :type: `str`
        """
        return self._topic

    def latest_offset(self):
        """Gets the latest offset for the partition.

        :return: Latest offset for the partition.
        :rtype: `int`
        """
        raise NotImplementedError

    def earliest_offset(self):
        """Gets the earliest offset for the partition.

        Due to logfile rotation, this will not always be 0. Instead,
        this will get the earliest offset for which the partition has data.

        :returns: The earliest offset for the partition.
        :rtype: `int`
        """
        raise NotImplementedError


class BaseTopic(object):
    """A Kafka topic."""

    @property
    def name(self):
        """The name of the topic.

        :type: `str`
        """
        return self._name

    @property
    def partitions(self):
        """The partitions of this topic.

        :type: `dict` of {`int`: :class:`kafka.base.BasePartition`}
        """
        return self._partitions

    def latest_offsets(self):
        """Get the latest offset for all partitions.

        :returns: The latest offset for all partitions in the topic.
        :rtype: `dict` of {:class:`kafka.base.BasePartition`: `int`}
        """
        raise NotImplementedError

    def earliest_offsets(self):
        """Get the earliest offset for all partitions.

        Due to logfile rotation, this will not always be 0. Instead,
        this will get the earliest offset for which the partition has data.

        :returns: The earliest offset for all partitions in the topic.
        :rtype: `dict` of {:class:`kafka.base.BasePartition`: `int`}
        """
        raise NotImplementedError


class BaseSimpleConsumer(object):
    """A simple consumer which reads data from a topic.

    This is a simple reader useful for testing or single-process situation.
    **If multiple processes use a SimpleConsumer and read the same topic,
    they will each read copies of the data.**  Instead, use a BalancedConsumer
    to ensure each message is only read once.

    The one advantage this implementation has over a BalancedConsumer is that
    the partitions to be read can be specified. Therefore, if one has hard
    coded which process reads which partitions, this is a useful soluton.
    """

    def __init__(self, client, topic, partitions=None):
        """Create a consumer for a topic.

        :param client: Client connection to the cluster.
        :type client: :class:`kafka.client.KafkaClient`
        :param topic: The topic to consume from.
        :type topic: :class:`kafka.abstract.Topic` or :class:`str`
        :param partitions: List of partitions to consume from.
        :type partitions: Iterable of :class:`kafka.abstract.Partition` or int
        """
        raise NotImplementedError

    def __iter__(self):
        """Iterator for messages in the consumer."""
        raise NotImplementedError

    @property
    def topic(self):
        """The topic from which data is being read.

        :type: :class:`kafka.base.BaseTopic`
        """
        return self._topic

    @property
    def partitions(self):
        """The partitions from which data is being read.

        :type: `dict` of {`int`: :class:`kafka.base.BasePartition`}
        """
        return self._partitions

    def consume(self, timeout=None):
        """Consume a message from the topic.

        :returns: A message.
        :rtype: :class:`kafka.common.Message`
        """
        raise NotImplementedError


class BaseProducer(object):
    """A producer which writes data to a topic.

    This producer is synchronous, waiting for a response from Kafka
    before returning. For an asynchronous implementation, use
    :class:`kafka.base.BaseAsyncProducer`
    """

    @property
    def topic(self):
        """The topic to which data is being written.

        :type: :class:`kafka.base.BaseTopic`
        """
        return self._topic

    @property
    def partitioner(self):
        """The partitioner used to determine which partition used.

        :type: :class:`kafka.partitioners.BasePartitioner`
        """
        return self._partitioner

    def produce(self, messages):
        """
        Produce messages to topic

        :type messages: Iterable of strings, or iterable of (key, value) tuples
                        to produce keyed messages
        """
        raise NotImplementedError
