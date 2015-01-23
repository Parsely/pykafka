from samsa import abstract


class Producer(abstract.Producer):

    def __init__(self, topic, partitioner=None):
        pass

    @property
    def topic(self):
        return self._topic

    @property
    def partitioner(self):
        return self._partitioner

    def produce(self, messages):
        pass
