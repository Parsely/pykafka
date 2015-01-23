import abc


class Cluster(object):
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


class Broker(object):
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


class Partition(object):
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

    @abc.abstractmethod
    def publish(self, data):
        """Publish data to this partition.

        TODO: Definition of what `data` is
        """
        pass

    @abc.abstractmethod
    def fetch(self, offset):
        """Fetch message or messages from this partition

        TODO: Figure out args and what this should support.
              It ought to be as simple as possible.
        """
        pass


class Consumer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, topic, partitions=None):
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


def Producer(object):
    __metaclass__ == abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, topic, partitioner=None):
        pass

    @abc.abstractproperty
    def topic(self):
        pass

    @abc.abstractproperty
    def partitioner(self):
        pass

    @abc.abstractmethod
    def produce(self, messages):
        pass


class Topic(object):
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
