from concurrent import futures
import logging
import weakref

from pykafka.exceptions import KafkaException
from pykafka.producer import Producer, CompressionType
from pykafka.utils.compat import get_bytes
from . import _rd_kafka


log = logging.getLogger(__name__)


class RdKafkaProducer(Producer):
    """A librdkafka-backed version of pykafka.Producer

    This aims to conform to the Producer interface as closely as possible.  One
    notable difference is that it returns a future from produce(), through
    which the user can later check if produced messages have made it to kafka.

    For an overview of how configuration keys are mapped to librdkafka's, see
    _mk_rdkafka_config_lists.
    """
    def start(self):
        if not self._running:
            brokers = b','.join(b.host + b":" + get_bytes(str(b.port))
                               for b in self._cluster.brokers.values())
            conf, topic_conf = self._mk_rdkafka_config_lists()

            self._rdk_producer = _rd_kafka.Producer()
            self._rdk_producer.configure(conf=conf)
            self._rdk_producer.configure(topic_conf=topic_conf)
            self._rdk_producer.start(brokers, self._topic.name)
            self._running = True

            def poll(self):
                try:
                    while self._running or self._rdk_producer.outq_len > 0:
                        self._rdk_producer.poll(timeout_ms=1000)
                except ReferenceError:  # weakref'd self
                    pass
                log.debug("Exiting RdKafkaProducer poller thread cleanly.")

            self._poller_thread = self._cluster.handler.spawn(
                    poll, args=(weakref.proxy(self), ))

    def _produce(self, message_partition_tup):
        (key, msg), part_id = message_partition_tup
        return self._rdk_producer.produce(msg, key, part_id)

    def _wait_all(self):
        # XXX should this have a timeout_ms param, or potentially wait forever?
        # XXX should raise exceptions for delivery errors (esp. when sync=True)
        not_done = True
        while not_done:
            done, not_done = futures.wait(
                self._rdk_producer._pending_futures.values(), timeout=1)
            if not_done:
                log.info("Waiting for incomplete Futures: {}".format(not_done))
                if not self._poller_thread.is_alive():
                    raise KafkaException("Poller thread dead, _wait_all would "
                                         "never finish.")

    def _mk_rdkafka_config_lists(self):
        """Populate conf, topic_conf to configure the rdkafka producer"""
        map_compression_types = {
            CompressionType.NONE: "none",
            CompressionType.GZIP: "gzip",
            CompressionType.SNAPPY: "snappy",
            }

        # For documentation purposes, all producer-relevant settings (all those
        # marked 'P' or '*') that appear in librdkafka/CONFIGURATION.md should
        # be listed below, in either `conf` or `topic_conf`, even if we do not
        # set them and they are commented out.

        conf = {  # destination: rd_kafka_conf_set
            "client.id": "pykafka.rdkafka",
            # Handled via rd_kafka_brokers_add instead:
            ##"metadata.broker.list"

            # NB these refer not to kafka messages, but to protocol messages.
            # We've no real equivalents for these, but defaults should be fine:
            ##"message.max.bytes"
            ##"receive.message.max.bytes"

            # No direct equivalents:
            ##"metadata.request.timeout.ms"
            ##"topic.metadata.refresh.interval.ms"
            ##"topic.metadata.refresh.fast.cnt"
            ##"topic.metadata.refresh.fast.interval.ms"
            ##"topic.metadata.refresh.sparse"

            ##"debug": "all",

            "socket.timeout.ms": self._cluster._socket_timeout_ms,
            ##"socket.send.buffer.bytes"
            ##"socket.receive.buffer.bytes"
            ##"socket.keepalive.enable"
            ##"socket.max.fails"
            ##"broker.address.ttl"
            ##"broker.address.family"

            # None of these are hooked up (yet):
            ##"statistics.interval.ms"
            ##"error_cb"
            ##"stats_cb"

            ##"log_cb"  # gets set in _rd_kafka module
            ##"log_level": 7,

            ##"socket_cb"
            ##"open_cb"
            ##"opaque"
            ##"internal.termination.signal"

            "queue.buffering.max.messages": self._max_queued_messages,
            "queue.buffering.max.ms": self._linger_ms,
            "message.send.max.retries": self._max_retries,
            "retry.backoff.ms": self._retry_backoff_ms,
            "compression.codec": map_compression_types[self._compression],
            "batch.num.messages": self._min_queued_messages,

            ##"delivery.report.only.error"
            ##"dr_cb"
            ##"dr_msg_cb"
            }
        topic_conf = {
            # see https://github.com/edenhill/librdkafka/issues/208
            "request.required.acks": (
                self._required_acks if self._required_acks <= 1 else -1),
            ##"enforce.isr.cnt"  # no current equivalent
            "request.timeout.ms": self._ack_timeout_ms,

            # This doesn't have an equivalent in pykafka.  It defaults to 300s
            # in librdkafka, which may be surprisingly long, so instead we
            # apply a formula to approach what you'd expect as a pykafka user:
            "message.timeout.ms": 1.2 * self._max_retries * (
                self._ack_timeout_ms + self._retry_backoff_ms),

            ##"produce.offset.report"
            ##"partitioner"  # dealt with in pykafka
            ##"opaque"
            }
        # librdkafka expects all config values as strings:
        conf = [(key, str(conf[key])) for key in conf]
        topic_conf = [(key, str(topic_conf[key])) for key in topic_conf]
        return conf, topic_conf
