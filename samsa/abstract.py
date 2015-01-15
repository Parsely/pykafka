import abc


class Broker(object):
    __metaclass__ = abc.ABCMeta
    pass

    @abc.abstractproperty
    def connected(self):
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
        pass

    @abc.abstractmethod
    def earliest_offsets(self):
        pass

    @abc.abstractmethod
    def publish(self, data):
        """Publish data to this topic.

        TODO: Definition of what `data` is
              Figure out how/where partitioner will be defined
              How are we going to support custom partitioners?
        """
        pass


# Do we want an abstract Message? Seems like both could use the same implementation.
class Message(object):
    """Message class.

    I'm not sure if this will be abstract, or just shared between the
    two implementations. Odds are that we'll make a copy of the message
    from C to Python (depends on when it's going to reclaim the memory).
    If that's the case, both implementations can share the Message, which
    will make life a lot easier.

    :ivar response_code: Response code from Kafka
    :ivar topic: Originating topic
    :ivar payload: Message payload
    :ivar key: (optional) Message key
    :ivar offset: Message offset
    """
    pass
