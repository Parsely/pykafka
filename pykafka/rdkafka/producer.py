import logging
from pkg_resources import parse_version

from pykafka.exceptions import RdKafkaStoppedException, ProducerStoppedException
from pykafka.producer import Producer, CompressionType
from pykafka.utils.compat import get_bytes
from . import _rd_kafka
from . import helpers


log = logging.getLogger(__name__)


class RdKafkaProducer(Producer):
    """A librdkafka-backed version of pykafka.Producer

    This aims to conform to the Producer interface as closely as possible.
    For an overview of how configuration keys are mapped to librdkafka's, see
    _mk_rdkafka_config_lists.

    Note: the `retry_backoff_ms` parameter behaves slightly differently in the
    `RdKafkaProducer` than it does in the pure Python `Producer`. In the rdkafka
    implementation, `retry_backoff_ms` indicates the exact time spent between message
    resend attempts, but in the pure Python version the time between attempts is also
    influenced by several other parameters, including `linger_ms` and `socket_timeout_ms`.

    Note also that `linger_ms` becomes the rdkafka-based producer's
    `queue.buffering.max.ms` config option and thus must be within the acceptable range
    defined by librdkafka. Several other parameters, including `ack_timeout_ms`,
    `max_retries`, and `retry_backoff_ms`, are used to derive certain librdkafka
    config values. Certain combinations of values for these parameters can result in
    configuration errors from librdkafka.

    The `broker_version` argument on `KafkaClient` must be set correctly to use the
    rdkafka producer.
    """
    def __init__(self,
                 cluster,
                 topic,
                 partitioner=None,
                 compression=CompressionType.NONE,
                 max_retries=3,
                 retry_backoff_ms=100,
                 required_acks=1,
                 ack_timeout_ms=10 * 1000,
                 max_queued_messages=100000,
                 min_queued_messages=2000,  # NB differs from pykafka.Producer
                 linger_ms=5 * 1000,
                 max_request_size=1000012,
                 sync=False,
                 delivery_reports=False,
                 auto_start=True,
                 serializer=None):
        callargs = {k: v for k, v in vars().items()
                    if k not in ("self", "__class__")}
        self._broker_version = cluster._broker_version
        self._rdk_producer = None
        self._poller_thread = None
        self._stop_poller_thread = cluster.handler.Event()
        # super() must come last because it calls start()
        super(RdKafkaProducer, self).__init__(**callargs)

    def start(self):
        if not self._running:
            brokers = b','.join(b.host + b":" + get_bytes(str(b.port))
                                for b in self._cluster.brokers.values())
            conf, topic_conf = self._mk_rdkafka_config_lists()

            self._rdk_producer = _rd_kafka.Producer()
            self._rdk_producer.configure(conf=conf)
            self._rdk_producer.configure(topic_conf=topic_conf)
            self._rdk_producer.start(
                brokers, self._topic.name, self._delivery_reports.put)
            self._running = True

            def poll(rdk_handle, stop_event):
                while not stop_event.is_set() or rdk_handle.outq_len() > 0:
                    rdk_handle.poll(timeout_ms=1000)
                log.debug("Exiting RdKafkaProducer poller thread cleanly.")

            self._stop_poller_thread.clear()
            self._poller_thread = self._cluster.handler.spawn(
                poll, args=(self._rdk_producer, self._stop_poller_thread))

    def stop(self):
        self._stop_poller_thread.set()
        super(RdKafkaProducer, self).stop()
        self._rdk_producer.stop()

    def _produce(self, message):
        try:
            self._rdk_producer.produce(message)
        except RdKafkaStoppedException:
            raise ProducerStoppedException

    def _wait_all(self):
        log.info("Blocking until all messages are sent")
        if self._poller_thread is not None:
            self._poller_thread.join()

    def _mk_rdkafka_config_lists(self):
        """Populate conf, topic_conf to configure the rdkafka producer"""
        map_compression_types = {
            CompressionType.NONE: "none",
            CompressionType.GZIP: "gzip",
            CompressionType.SNAPPY: "snappy",
            CompressionType.LZ4: "lz4",
        }

        # For documentation purposes, all producer-relevant settings (all those
        # marked 'P' or '*') that appear in librdkafka/CONFIGURATION.md should
        # be listed below, in either `conf` or `topic_conf`, even if we do not
        # set them and they are commented out.
        ver10 = parse_version(self._broker_version) >= parse_version("0.10.0")
        conf = {  # destination: rd_kafka_conf_set
            "client.id": "pykafka.rdkafka",
            # Handled via rd_kafka_brokers_add instead:
            # "metadata.broker.list"

            # NB these refer not to payloads, but to wire messages
            # We've no real equivalents for these, but defaults should be fine:
            # "receive.message.max.bytes"

            # No direct equivalents:
            # "metadata.request.timeout.ms"
            # "topic.metadata.refresh.interval.ms"
            # "topic.metadata.refresh.fast.cnt"
            # "topic.metadata.refresh.fast.interval.ms"
            # "topic.metadata.refresh.sparse"

            # "debug": "all",

            "socket.timeout.ms": self._cluster._socket_timeout_ms,
            # "socket.send.buffer.bytes"
            # "socket.receive.buffer.bytes"
            # "socket.keepalive.enable"
            # "socket.max.fails"
            # "broker.address.ttl"
            # "broker.address.family"

            # None of these need to be hooked up
            # "statistics.interval.ms"
            # "error_cb"  # we let errors be reported via log_cb
            # "stats_cb"

            # "log_cb"  # gets set in _rd_kafka module
            # "log_level": 7,

            # "socket_cb"
            # "open_cb"
            # "opaque"
            # "internal.termination.signal"
            # Do not log connection.close, which may be caused by
            # connections.max.idle.ms
            # Cf https://github.com/edenhill/librdkafka/issues/437
            "log.connection.close": "false",

            "queue.buffering.max.messages": self._max_queued_messages,
            "queue.buffering.max.ms": self._linger_ms,
            "message.send.max.retries": self._max_retries,
            "retry.backoff.ms": self._retry_backoff_ms,
            "compression.codec": map_compression_types[self._compression],
            "batch.num.messages": self._min_queued_messages,
            "message.max.bytes": self._max_request_size,

            # Report successful and failed messages so we know to dealloc them
            "delivery.report.only.error": "false",
            "api.version.request": ver10,
            # "dr_cb"
            # "dr_msg_cb"  # gets set in _rd_kafka module
        }
        # broker.version.fallback is incompatible with >-0.10
        if not ver10:
            conf["broker.version.fallback"] = self._broker_version
        conf.update(helpers.rdk_ssl_config(self._cluster))

        topic_conf = {
            # see https://github.com/edenhill/librdkafka/issues/208
            "request.required.acks": (
                self._required_acks if self._required_acks <= 1 else -1),
            # "enforce.isr.cnt"  # no current equivalent
            "request.timeout.ms": self._ack_timeout_ms,

            # This doesn't have an equivalent in pykafka.  It defaults to 300s
            # in librdkafka, which may be surprisingly long, so instead we
            # apply a formula to approach what you'd expect as a pykafka user:
            "message.timeout.ms": 1.2 * self._max_retries * (
                self._ack_timeout_ms + self._retry_backoff_ms),

            "produce.offset.report": self._delivery_reports.queue is not None,
            # "partitioner"  # dealt with in pykafka
            # "opaque"
        }
        # librdkafka expects all config values as strings:
        conf = [(key, str(conf[key])) for key in conf]
        topic_conf = [(key, str(topic_conf[key])) for key in topic_conf]
        return conf, topic_conf
