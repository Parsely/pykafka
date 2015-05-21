from pykafka.simpleconsumer import SimpleConsumer
from . import _rd_kafka


class RdKafkaSimpleConsumer(SimpleConsumer):

    def _setup_fetch_workers(self):
        brokers = ','.join(map(lambda b: ':'.join((b.host, str(b.port))),
                               self._cluster.brokers.values()))
        return _rd_kafka.Consumer(brokers,
                                  self._topic.name,
                                  self._partitions_by_id.keys())

    def consume(self, block=True):
        timeout_ms = -1 if block else self._consumer_timeout_ms
        # TODO check '-1' is supported in librdkafka (not clearly documented)
        return self._fetch_workers.consume()  # TODO pass timeout arg

    def stop(self):
        super(RdKafkaSimpleConsumer, self).stop()
        self._fetch_workers = None
        # TODO _rd_kafka.Consumer needs an explicit stop() method; relying on
        # gc here doesn't tell us whether stopping succeeded
