from logging import Handler


class KafkaHandler(Handler):
    """
    Basic Kafka-based logging handler.

    :param topic: topic to publish to
    :type topic: :class:`samsa.topics.Topic`
    """
    def __init__(self, topic, *args, **kwargs):
        Handler.__init__(self, *args, **kwargs)  # avoid `super` for 2.6
        self.topic = topic

    def emit(self, record):
        self.topic.publish(self.format(record))
