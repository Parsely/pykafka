from samsa import abstract


class Consumer(abstract.Consumer):

    def __init__(self, client, topic, partitions=None):
        pass

    @property
    def topic(self):
        return self._topic

    @property
    def partitions(self):
        return self._partitions

    def __iter__(self):
        pass

    def consume(self, timeout=None):
        pass
