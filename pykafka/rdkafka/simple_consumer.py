from collections import namedtuple

from pykafka.simpleconsumer import SimpleConsumer
from . import _rd_kafka


# field names compatible with pykafka.protocol.Message:
Message = namedtuple("Message",
                     ("value", "partition_key", "partition_id", "offset"))


class RdKafkaSimpleConsumer(SimpleConsumer):

    def _setup_fetch_workers(self):
        brokers = ','.join(map(lambda b: ':'.join((b.host, str(b.port))),
                               self._cluster.brokers.values()))
        partition_ids = list(self._partitions_by_id.keys())
        start_offsets = [
            self._partitions_by_id[p].next_offset for p in partition_ids]
        return _rd_kafka.Consumer(brokers,
                                  self._topic.name,
                                  partition_ids,
                                  start_offsets)

    def consume(self, block=True):
        timeout_ms = -1 if block else self._consumer_timeout_ms
        msg = Message(* self._fetch_workers.consume(timeout_ms))

        # set offset in OwnedPartition so the autocommit_worker can find it
        self._partitions_by_id[msg.partition_id].set_offset(msg.offset)
        return msg

    def stop(self):
        super(RdKafkaSimpleConsumer, self).stop()
        self._fetch_workers = None
        # TODO _rd_kafka.Consumer needs an explicit stop() method; relying on
        # gc here doesn't tell us whether stopping succeeded
