from collections import namedtuple
import logging

from pykafka import base
from .config import convert_config
from .utils import get_defaults_dict

try:
    import rd_kafka
except ImportError:
    pass # not installed


logger = logging.getLogger(__name__)


Message = namedtuple("Message", ["topic", "payload", "key", "offset"])
# TODO ^^ namedtuple is just a placeholder thingy until we've fleshed out
#      kafka.common.Message etc


class SimpleConsumer(base.BaseSimpleConsumer):

    DEFAULTS = get_defaults_dict(base.BaseSimpleConsumer.__init__)

    def __init__(
            self,
            topic,
            cluster,
            consumer_group=DEFAULTS["consumer_group"],
            partitions=None,
            fetch_message_max_bytes=DEFAULTS["fetch_message_max_bytes"],
            num_consumer_fetchers=DEFAULTS["num_consumer_fetchers"],
            auto_commit_enable=DEFAULTS["auto_commit_enable"],
            auto_commit_interval_ms=DEFAULTS["auto_commit_interval_ms"],
            queued_max_messages=DEFAULTS["queued_max_messages"],
            fetch_min_bytes=DEFAULTS["fetch_min_bytes"],
            fetch_wait_max_ms=DEFAULTS["fetch_wait_max_ms"],
            refresh_leader_backoff_ms=DEFAULTS["refresh_leader_backoff_ms"],
            offsets_channel_backoff_ms=DEFAULTS["offsets_channel_backoff_ms"],
            offsets_commit_max_retries=DEFAULTS["offsets_commit_max_retries"],
            auto_offset_reset=DEFAULTS["auto_offset_reset"],
            consumer_timeout_ms=DEFAULTS["consumer_timeout_ms"]):

        self._topic = (topic
                       if not isinstance(topic, basestring)
                       else cluster.topics[topic])

        # In pykafka, consumer_timeout_ms is an init-time config option, but
        # in rdkafka it is a call parameter.  We'll save it for call time:
        self.consumer_timeout_ms = consumer_timeout_ms

        # Now, convert callargs to config dicts that we can pass to rd_kafka:
        config_callargs = {k: v for k, v in vars().items() if (k not in (
            "self", "topic", "cluster", "partitions", "consumer_timeout_ms"))}
        config, topic_config = convert_config(
            config_callargs, base_config=self.topic.cluster.config)

        rdk_consumer = rd_kafka.Consumer(config)
        self.rdk_topic = rdk_consumer.open_topic(self.topic.name, topic_config)
        self.rdk_queue = rdk_consumer.new_queue()

        for p in (partitions or self.topic.partitions.keys()):
            if hasattr(p, 'id'):  # seems we've been handed a BasePartition, so:
                p = p.id
            self.rdk_queue.add_toppar(self.rdk_topic, p, start_offset=0)
            # FIXME ^^ change python-librdkafka to provide default for offset
            #       (which should probably be OFFSET_STORED)

        # Note that this ^^ uses a new rdk_consumer handle for every instance;
        # this avoids the confusion of not being allowed a second reader on
        # the same toppar (a restriction python-librdkafka would impose if
        # we'd use a common rdk_consumer).  The extra overhead should be
        # acceptable for most uses.

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
