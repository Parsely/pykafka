from pykafka.simpleconsumer import SimpleConsumer
from . import _rd_kafka


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
        timeout_ms = self._consumer_timeout_ms if block else 1
        msg = self._fetch_workers.consume(timeout_ms)
        if msg is not None:
            # set offset in OwnedPartition so the autocommit_worker can find it
            self._partitions_by_id[msg.partition_id].set_offset(msg.offset)
        return msg

    def stop(self):
        super(RdKafkaSimpleConsumer, self).stop()
        self._fetch_workers = None
        # TODO _rd_kafka.Consumer needs an explicit stop() method; relying on
        # gc here doesn't tell us whether stopping succeeded
