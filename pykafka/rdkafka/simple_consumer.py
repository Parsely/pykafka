from contextlib import contextmanager

from pykafka.simpleconsumer import (
    SimpleConsumer, ConsumerStoppedException, OffsetType)
from . import _rd_kafka


class RdKafkaSimpleConsumer(SimpleConsumer):
    """A pykafka.SimpleConsumer with librdkafka-based fetchers

    This aims to conform to the SimpleConsumer interface as closely as
    possible.  There are some notable differences:

    1. rotating over partitions: while message ordering within partitions is
       conserved (of course!), the order in which partitions are visited will
       deviate.  In particular, here we may emit more than one message from
       the same partition before visiting another
    2. ignores num_consumer_fetchers: librdkafka will typically spawn at least
       as many threads as there are kafka cluster nodes

    For an overview of how configuration keys are mapped to librdkafka's, see
    _mk_rdkafka_config_lists.
    """

    def _setup_fetch_workers(self):
        brokers = ','.join(map(lambda b: ':'.join((b.host, str(b.port))),
                               self._cluster.brokers.values()))
        partition_ids = list(self._partitions_by_id.keys())
        start_offsets = [
            self._partitions_by_id[p].next_offset for p in partition_ids]
        conf, topic_conf = self._mk_rdkafka_config_lists()

        self._rdk_consumer = _rd_kafka.Consumer()
        self._rdk_consumer.configure(conf=conf)
        self._rdk_consumer.configure(topic_conf=topic_conf)
        self._rdk_consumer.start(brokers,
                                 self._topic.name,
                                 partition_ids,
                                 start_offsets)

    def consume(self, block=True):
        timeout_ms = self._consumer_timeout_ms if block else 1
        try:
            msg = self._rdk_consumer.consume(timeout_ms)
        except _rd_kafka.ConsumerStoppedException:
            raise ConsumerStoppedException
        if msg is not None:
            # set offset in OwnedPartition so the autocommit_worker can find it
            self._partitions_by_id[msg.partition_id].set_offset(msg.offset)
        return msg

    def stop(self):
        if hasattr(self, "_rdk_consumer") and self._rdk_consumer is not None:
            # Call _rd_kafka.Consumer.stop explicitly, so we may catch errors:
            self._rdk_consumer.stop()
            self._rdk_consumer = None
        super(RdKafkaSimpleConsumer, self).stop()

    def fetch_offsets(self):
        # Restart, because _rdk_consumer needs its internal offsets resynced
        with self._stop_start_rdk_consumer():
            return super(RdKafkaSimpleConsumer, self).fetch_offsets()

    def reset_offsets(self, partition_offsets=None):
        # Restart, because _rdk_consumer needs its internal offsets resynced
        with self._stop_start_rdk_consumer():
            super(RdKafkaSimpleConsumer, self).reset_offsets(partition_offsets)

    @contextmanager
    def _stop_start_rdk_consumer(self):
        """Context manager for methods to temporarily stop _rdk_consumer"""
        restart_required = (self._running and hasattr(self, "_rdk_consumer")
                            and self._rdk_consumer is not None)
        if restart_required:
            # Note we must not call a full self.stop() as that would stop
            # SimpleConsumer threads too, and if we'd have to start() again
            # that could have other side effects (eg resetting offsets).
            self._rdk_consumer.stop()
            restart_required = True
        yield
        if restart_required:
            self._setup_fetch_workers()

    def _mk_rdkafka_config_lists(self):
        """Populate conf, topic_conf to configure the rdkafka consumer"""
        # For documentation purposes, all consumer-relevant settings (all those
        # marked 'C' or '*') that appear in librdkafka/CONFIGURATION.md should
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

            ##"debug"

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
            ##"log_cb"
            ##"log_level"

            ##"socket_cb"
            ##"open_cb"
            ##"opaque"
            ##"internal.termination.signal"

            # Although the names seem to disagree, SimpleConsumer currently
            # uses _queued_max_messages in a way analogous to
            # queued.min.messages; that is "keep trying to fetch until there's
            # this number of messages on the queue".  There's no equivalent of
            # queued.max.messages.kbytes so for now we infer the implied
            # maximum (which, with default settings, is ~2GB per partition):
            "queued.min.messages": self._queued_max_messages,
            "queued.max.messages.kbytes": str(
                self._queued_max_messages
                * self._fetch_message_max_bytes // 1024),

            "fetch.wait.max.ms": self._fetch_wait_max_ms,
            "fetch.message.max.bytes": self._fetch_message_max_bytes,
            "fetch.min.bytes": self._fetch_min_bytes,
            ##"fetch.error.backoff.ms"  # currently no real equivalent for it

            # We're outsourcing message fetching, but not offset management or
            # consumer rebalancing to librdkafka.  Thus, consumer-group id
            # *must* remain unset, or we would appear to be two consumer
            # instances to the kafka cluster:
            ##"group.id"
            }

        map_offset_types = {
            OffsetType.EARLIEST: "smallest",
            OffsetType.LATEST: "largest",
            }
        topic_conf = {
            ##"opaque"
            ##"group.id"
            "auto.commit.enable": "false",
            ##"auto.commit.interval.ms"
            "auto.offset.reset": map_offset_types[self._auto_offset_reset],
            ##"offset.store.path"
            ##"offset.store.sync.interval.ms"
            ##"offset.store.method"
            }
        # librdkafka expects all config values as strings:
        conf = [(key, str(conf[key])) for key in conf]
        topic_conf = [(key, str(topic_conf[key])) for key in topic_conf]
        return conf, topic_conf
