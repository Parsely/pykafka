from kafka.pykafka.simpleconsumer import SimpleConsumer


class BalancedConsumer():
    def __init__(self,
                 topic,
                 cluster,
                 consumer_group):
        """Create a BalancedConsumer

        :param topic: the topic this consumer should consume
        :type topic: pykafka.topic.Topic
        :param cluster: the cluster this consumer should connect to
        :type cluster: pykafka.cluster.Cluster
        :param consumer_group: the name of the consumer group to join
        :type consumer_group: str
        """
        self._cluster = cluster
        self._consumer_group = consumer_group
        self._topic = topic

    def _setup_internal_consumer(self):
        participants = self._get_participants()
        partitions = self._decide_partitions(participants)
        return SimpleConsumer(self._topic,
                              self._cluster,
                              consumer_group=self._consumer_group,
                              partitions=partitions)

    def _decide_partitions(self, participants):
        return []

    def _get_participants(self):
        return []
