from copy import copy
import logging
from time import clock

from kafka import base, partitioners
from kafka .exceptions import KafkaException

try:
    import rd_kafka
except ImportError:
    pass # not installed


logger = logging.getLogger(__name__)


class Producer(base.BaseProducer):

    def __init__(self, topic, partitioner=partitioners.random_partitioner):
        self._topic = topic
        self._partitioner = partitioner

        config, topic_config = self._configure()
        rdk_producer = rd_kafka.Producer(config)
        self.rdk_topic = rdk_producer.open_topic(self.topic.name, topic_config)

    def _configure(self):
        config = copy(self.topic.cluster.config)
        topic_config = {} # TODO where do we expose this?
        # TODO config.update( ...stuff specific to this Producer ...)

        def delivery_callback(msg, **kwargs):
            # cf Producer.produce() below to get what this is for
            msg.opaque.append(msg.cdata.err)

        if "dr_msg_cb" in config:
            logger.warning("Overwriting user-set delivery callback with ours.")
        config["dr_msg_cb"] = delivery_callback

        # We'll reuse this librdkafka parameter to set how long we'll wait for
        # delivery reports (300000 is the current librdkafka default):
        # TODO move the default to some config header
        self.message_timeout_ms = config.get("message.timeout.ms", 300 * 1000)

        return config, topic_config

    @property
    def topic(self):
        return self._topic

    @property
    def partitioner(self):
        return self._partitioner

    def produce(self, messages):
        """ Sync-producer: raises exceptions on delivery failures """
        delivery_reports = [list() for _ in range(len(messages))]
        # This ^^ list of lists is perhaps overly cautious and bulky.  It's
        # meant to provide thread-safety without locking, but maybe all
        # delivery callbacks will actually run on one thread.

        for msg, dr in zip(messages, delivery_reports):
            par = self.partitioner(self.topic.partitions.keys(),
                                   msg.partition_key)
            self.rdk_topic.produce(msg.value,
                                   partition=par,
                                   msg_opaque=dr)
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
