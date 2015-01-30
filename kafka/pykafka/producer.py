from kafka import base, partitioners

from .protocol import Message, PartitionProduceRequest

class AsyncProducer(base.BaseAsyncProducer):

    def __init__(self,
                 topic,
                 partitioner=None,
                 compression=None,
                 max_retries=3,
                 retry_backoff_ms=100,
                 topic_refresh_interval_ms=600000,
                 batch_size=200,
                 batch_time_ms=5000,
                 max_pending_messages=10000):
        pass

class Producer(base.BaseProducer):

    def __init__(self,
                 topic,
                 partitioner=None,
                 compression=None,
                 max_retries=3,
                 retry_backoff_ms=100,
                 topic_refresh_interval_ms=600000):
        self._partitioner = partitioner or partitioners.random_partitioner
        self._topic = topic
        self._max_retries = max_retries
        self._retry_backoff_ms = retry_backoff_ms
        self._topic_refresh_interval_ms = topic_metadata_refresh_interval_ms

    @property
    def topic(self):
        return self._topic

    @property
    def partitioner(self):
        return self._partitioner

    def produce(self, data):
        target = self._partitioner(self._topic.partitions.values(), None)
        broker = target.leader
        messages = (Message(d) for d in data)
        req = PartitionProduceRequest(self._topic.name, target.id, messages)
        target.leader.produce_messages([req])
