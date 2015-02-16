from collections import namedtuple
from copy import copy
import logging

from kafka import base

try:
    import rd_kafka
except ImportError:
    pass # not installed


logger = logging.getLogger(__name__)


Message = namedtuple("Message", ["topic", "payload", "key", "offset"])
# TODO ^^ namedtuple is just a placeholder thingy until we've fleshed out
#      kafka.common.Message etc


class SimpleConsumer(base.BaseSimpleConsumer):

    def __init__(self, client, topic, partitions=None):
        if isinstance(topic, basestring):
            topic = client[topic]
        self._topic = topic

        config, topic_config = self._configure()
        rdk_consumer = rd_kafka.Consumer(config)
        self.rdk_topic = rdk_consumer.open_topic(self.topic.name, topic_config)
        self.rdk_queue = rdk_consumer.new_queue()

        for p in (partitions or self.topic.partitions.keys()):
            self.rdk_queue.add_toppar(self.rdk_topic, p, start_offset=0)
            # FIXME ^^ change python-librdkafka to provide default for offset
            #       (which should probably be OFFSET_STORED)

        # Note that this ^^ uses a new rdk_consumer handle for every instance;
        # this avoids the confusion of not being allowed a second reader on
        # the same toppar (a restriction python-librdkafka would impose if
        # we'd use a common rdk_consumer).  The extra overhead should be
        # acceptable for most uses.

    def _configure(self):
        config = copy(self.topic.cluster.config)
        topic_config = {} # TODO where do we expose this?
        # TODO config.update( ...stuff like group.id ...)

        return config, topic_config

    @property
    def partitions(self):
        partition_ids = [p.partition_id for p in self.rdk_queue.partitions]
        return {k: v for k, v in self.topic.partitions.items()
                              if k in partition_ids}

    def __iter__(self):
        raise NotImplementedError
        # TODO implement StopIteration in python-librdkafka

    def consume(self, timeout=1):
        msg = self.rdk_queue.consume(timeout_ms=1000 * timeout)
        return None if msg is None else Message(topic=self.topic.name,
                                                key=msg.key[:],
                                                payload=msg.payload[:],
                                                offset=msg.offset)
