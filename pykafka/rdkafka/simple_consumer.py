from contextlib import contextmanager
import logging
from pkg_resources import parse_version
import sys
import time

from pykafka.exceptions import RdKafkaStoppedException, ConsumerStoppedException
from pykafka.simpleconsumer import SimpleConsumer, OffsetType
from pykafka.utils.compat import get_bytes
from pykafka.utils.error_handlers import valid_int
from . import _rd_kafka
from . import helpers

log = logging.getLogger(__name__)


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

    The `broker_version` argument on `KafkaClient` must be set correctly to use the
    rdkafka consumer.
    """
    def __init__(self,
                 topic,
                 cluster,
                 consumer_group=None,
                 partitions=None,
                 fetch_message_max_bytes=1024 * 1024,
                 num_consumer_fetchers=1,
                 auto_commit_enable=False,
                 auto_commit_interval_ms=60 * 1000,
                 queued_max_messages=10**5,  # NB differs from SimpleConsumer
                 fetch_min_bytes=1,
                 fetch_error_backoff_ms=500,
                 fetch_wait_max_ms=100,
                 offsets_channel_backoff_ms=1000,
                 offsets_commit_max_retries=5,
                 auto_offset_reset=OffsetType.EARLIEST,
                 consumer_timeout_ms=-1,
                 auto_start=True,
                 reset_offset_on_start=False,
                 compacted_topic=False,
                 generation_id=-1,
                 consumer_id=b'',
                 deserializer=None,
                 reset_offset_on_fetch=True):
        callargs = {k: v for k, v in vars().items()
                         if k not in ("self", "__class__")}
        self._rdk_consumer = None
        self._poller_thread = None
        self._stop_poller_thread = cluster.handler.Event()
        self._broker_version = cluster._broker_version
        self._fetch_error_backoff_ms = valid_int(fetch_error_backoff_ms)
        # super() must come last for the case where auto_start=True
        super(RdKafkaSimpleConsumer, self).__init__(**callargs)

    def _setup_fetch_workers(self):
        brokers = b','.join(b.host + b":" + get_bytes(str(b.port))
                           for b in self._cluster.brokers.values())
        partition_ids = list(self._partitions_by_id.keys())
        start_offsets = [
            self._partitions_by_id[p].next_offset for p in partition_ids]
        conf, topic_conf = self._mk_rdkafka_config_lists()

        self._rdk_consumer = _rd_kafka.Consumer()
        log.debug("Configuring _rdk_consumer...")
        self._rdk_consumer.configure(conf=conf)
        self._rdk_consumer.configure(topic_conf=topic_conf)

        start_kwargs = {"brokers": brokers,
                        "topic_name": self._topic.name,
                        "partition_ids": partition_ids,
                        "start_offsets": start_offsets}
        log.debug("Starting _rdk_consumer with {}".format(start_kwargs))
        self._rdk_consumer.start(**start_kwargs)

        # Poll: for a consumer, the main reason to poll the handle is that
        # this de-queues log messages at error level that might otherwise be
        # held up in librdkafka
        def poll(rdk_handle, stop_event):
            while not stop_event.is_set():
                try:
                    rdk_handle.poll(timeout_ms=1000)
                except RdKafkaStoppedException:
                    break
                except:
                    self._worker_exception = sys.exc_info()
            log.debug("Exiting RdKafkaSimpleConsumer poller thread cleanly.")

        self._stop_poller_thread.clear()
        self._poller_thread = self._cluster.handler.spawn(
            poll, args=(self._rdk_consumer, self._stop_poller_thread))

    def consume(self, block=True, unblock_event=None):
        timeout_ms = self._consumer_timeout_ms if block else 1
        try:
            msg = self._consume(timeout_ms, unblock_event)
        # if _rdk_consumer is None we'll catch an AttributeError here
        except (RdKafkaStoppedException, AttributeError) as e:
            if not self._running:
                raise ConsumerStoppedException
            else:  # unexpected other reason
                raise
        else:
            if not self._running:
                # Even if we did get a msg back, we'll still want to abort
                # here, because the new offset wouldn't get committed anymore
                raise ConsumerStoppedException
        if msg is not None:
            # set offset in OwnedPartition so the autocommit_worker can find it
            self._partitions_by_id[msg.partition_id].set_offset(msg.offset)
        return msg

    def _consume(self, timeout_ms, unblock_event):
        """Helper to allow catching interrupts around rd_kafka_consume"""
        inner_timeout_ms = 500  # unblock at this interval at least

        if timeout_ms < 0:
            while True:
                self._raise_worker_exceptions()
                if unblock_event and unblock_event.is_set():
                    return
                msg = self._rdk_consumer.consume(inner_timeout_ms)
                if msg is not None:
                    return msg
        else:
            t_start = time.time()
            leftover_ms = timeout_ms
            while leftover_ms > 0:
                self._raise_worker_exceptions()
                if unblock_event and unblock_event.is_set():
                    return
                inner_timeout_ms = int(min(leftover_ms, inner_timeout_ms))
                msg = self._rdk_consumer.consume(inner_timeout_ms)
                if msg is not None:
                    return msg
                elapsed_ms = 1000 * (time.time() - t_start)
                leftover_ms = timeout_ms - elapsed_ms

    def stop(self):
        # NB we should always call super() first, because it takes care of
        # shipping all important state (ie stored offsets) out
        ret = super(RdKafkaSimpleConsumer, self).stop()
        if self._rdk_consumer is not None:
            self._stop_poller_thread.set()
            # Call _rd_kafka.Consumer.stop explicitly, so we may catch errors:
            self._rdk_consumer.stop()
            log.debug("Issued stop to _rdk_consumer.")
            self._rdk_consumer = None
        return ret

    def fetch_offsets(self):
        # Restart, because _rdk_consumer needs its internal offsets resynced
        with self._stop_start_rdk_consumer():
            return super(RdKafkaSimpleConsumer, self).fetch_offsets()

    def reset_offsets(self, partition_offsets=None):
        # Restart, because _rdk_consumer needs its internal offsets resynced
        with self._stop_start_rdk_consumer():
            return super(
                RdKafkaSimpleConsumer, self).reset_offsets(partition_offsets)

    @contextmanager
    def _stop_start_rdk_consumer(self):
        """Context manager for methods to temporarily stop _rdk_consumer

        We need this because we hold read-offsets both in pykafka and
        internally in _rdk_consumer.  We'll hold the one in pykafka to be the
        ultimate source of truth.  So whenever offsets are to be changed (other
        than through consume() that is), we need to clobber _rdk_consumer.
        """
        restart_required = self._running and self._rdk_consumer is not None
        if restart_required:
            # Note we must not call a full self.stop() as that would stop
            # SimpleConsumer threads too, and if we'd have to start() again
            # that could have other side effects (eg resetting offsets).
            self._rdk_consumer.stop()
            log.debug("Temporarily stopped _rdk_consumer.")
        yield
        if restart_required:
            self._setup_fetch_workers()
            log.debug("Restarted _rdk_consumer.")

    def _mk_rdkafka_config_lists(self):
        """Populate conf, topic_conf to configure the rdkafka consumer"""
        # For documentation purposes, all consumer-relevant settings (all those
        # marked 'C' or '*') that appear in librdkafka/CONFIGURATION.md should
        # be listed below, in either `conf` or `topic_conf`, even if we do not
        # set them and they are commented out.
        ver10 = parse_version(self._broker_version) >= parse_version("0.10.0")
        conf = {  # destination: rd_kafka_conf_set
            "client.id": "pykafka.rdkafka",
            # Handled via rd_kafka_brokers_add instead:
            ##"metadata.broker.list"

            # NB these refer not to payloads, but to wire messages
            ##"message.max.bytes"  # leave at default
            "receive.message.max.bytes": (  # ~ sum of PartitionFetchRequests
                self._fetch_message_max_bytes * (len(self.partitions) + 1)),

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

            # None of these need to be hooked up
            ##"statistics.interval.ms"
            ##"error_cb"  # we let errors be reported via log_cb
            ##"stats_cb"

            ##"log_cb"  # gets set in _rd_kafka module
            ##"log_level": 7,

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
            "fetch.error.backoff.ms": self._fetch_error_backoff_ms,
            "api.version.request": ver10,

            # We're outsourcing message fetching, but not offset management or
            # consumer rebalancing to librdkafka.  Thus, consumer-group id
            # *must* remain unset, or we would appear to be two consumer
            # instances to the kafka cluster:
            ##"group.id"
            }
        # broker.version.fallback is incompatible with >-0.10
        if not ver10:
            conf["broker.version.fallback"] = self._broker_version
        conf.update(helpers.rdk_ssl_config(self._cluster))

        map_offset_types = {
            OffsetType.EARLIEST: "smallest",
            OffsetType.LATEST: "largest",
            }
        topic_conf = {
            ##"opaque"
            ##"group.id"  # see note above re group.id

            # pykafka handles offset commits
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
