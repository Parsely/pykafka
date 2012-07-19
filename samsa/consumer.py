class Consumer(object):
    def __init__(self, cluster, topic, group):
        self.cluster = cluster
        self.topic = topic
        self.group = group

    def __iter__(self):
        """
        Returns an iterator of messages.
        """
        raise NotImplementedError
