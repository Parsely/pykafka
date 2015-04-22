import logging
from time import clock

from pykafka import base
from pykafka.exceptions import KafkaException
from .config import convert_config, default_topic_config
from .utils import get_defaults_dict

try:
    import rd_kafka
except ImportError:
    pass # not installed


logger = logging.getLogger(__name__)
BASE_PRODUCER_DEFAULTS = get_defaults_dict(base.BaseProducer.__init__)


class Producer(base.BaseProducer):

    def __init__(
            self,
            client,
            topic,
            partitioner=BASE_PRODUCER_DEFAULTS["partitioner"],
            compression=BASE_PRODUCER_DEFAULTS["compression"],
            max_retries=BASE_PRODUCER_DEFAULTS["max_retries"],
            retry_backoff_ms=BASE_PRODUCER_DEFAULTS["retry_backoff_ms"],
            topic_refresh_interval_ms=(
                BASE_PRODUCER_DEFAULTS["topic_refresh_interval_ms"]),
            required_acks=BASE_PRODUCER_DEFAULTS["required_acks"],
            ack_timeout_ms=BASE_PRODUCER_DEFAULTS["ack_timeout_ms"],
            batch_size=BASE_PRODUCER_DEFAULTS["batch_size"]):
        self.client = client
        self._topic = (topic
                       if not isinstance(topic, basestring)
                       else self.client.topics[topic])
        self._partitioner = partitioner

        # Now, convert callargs to config dicts that we can pass to rd_kafka:
        config_callargs = {k: v for k, v in vars().items() if (
            k not in ("self", "client", "topic", "partitioner"))}
        config, topic_config = convert_config(
            config_callargs, base_config=self.topic.cluster.config)

        def delivery_callback(msg, **kwargs):
            # cf Producer.produce() below to get what this is for
            msg.opaque.append(msg.cdata.err)

        if "dr_msg_cb" in config:
            logger.warning("Overwriting user-set delivery callback with ours.")
        config["dr_msg_cb"] = delivery_callback

        # Reuse this parameter to set how long to wait for delivery reports:
        self.message_timeout_ms = int(
                topic_config.get("message.timeout.ms",
                                 default_topic_config()["message.timeout.ms"]))

        # Finally... open topic:
        rdk_producer = rd_kafka.Producer(config)
        self.rdk_topic = rdk_producer.open_topic(self.topic.name, topic_config)

    @property
    def topic(self):
        return self._topic

    @property
    def partitioner(self):
        return self._partitioner

    def produce(self, messages):
        """ Sync-producer: raises exceptions on delivery failures """
        delivery_reports = []
        # This ^^ list of lists is perhaps overly cautious and bulky.  It's
        # meant to provide thread-safety without locking, but maybe all
        # delivery callbacks will actually run on one thread.

        for msg in messages:
            key, msg = (None, msg) if isinstance(msg, bytes) else msg
            par = self.partitioner(self.topic.partitions.keys(), key)
            delivery_reports.append([])
            self.rdk_topic.produce(msg,
                                   partition=par,
                                   msg_opaque=delivery_reports[-1])
        # XXX There may be some batch size at which it becomes more efficient
        #     to call rd_kafka_produce_batch() instead; or perhaps there isn't,
        #     because in that approach we'd have to wrap self.partitioner in a
        #     partitioner-callback.  We should test both implementations.

        timeout_at = clock() + self.message_timeout_ms * .0001
        while [] in delivery_reports: # ie still waiting for some callbacks
            if clock() > timeout_at: break
            self.rdk_topic.kafka_handle.poll()

        failures = False
        for i, dr in enumerate(delivery_reports):
            if not dr:
                logger.error("Delivery report timed out for msg {}".format(i))
                failures = True
            elif dr[0] != 0:
                logger.error("Delivery error {} for msg {}".format(dr[0], i))
                failures = True
            else:
                logger.debug("Successful delivery: {}".format(i))
        if failures:
            raise KafkaException("Failed to deliver (some) messages; see log "
                                 "output for details")
