from kafka import abstract, partitioners

from .protocol import Message, PartitionProduceRequest


class Producer(abstract.Producer):

    def __init__(self, topic, partitioner=None):
        self._partitioner = partitioner or partitioners.random_partitioner
        self._topic = topic

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
