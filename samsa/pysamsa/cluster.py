class Cluster(object):
    """Simple Cluster implementation used to populate the SamsaClient."""

    def __init__(self, hosts, handler):
        self._seeds = hosts
        self._timeout = timeout
        self._handler = handler
        self.brokers = None
        self.topics = None
        self.update()

    @property
    def brokers(self):
        return self._brokers

    @property
    def topics(self):
        return self._topics

    def _get_metadata(self):
        """Get fresh cluster metadata from a broker"""
        if self.brokers:
            brokers = self.brokers.values()
        else:
            brokers = [Broker(-1, host, port, self.handler, self._timeout)
                       for host, port
                       in [h.split(':') for h in self._seed_hosts.split(',')]]
        for broker in brokers:
            try:
                return broker.request_metadata()
            except Exception, e:
                logger.warning('Unable to connect to broker %s:%s',
                               broker.host, broker.port)
                raise
        raise Exception('Unable to connect to a broker to fetch metadata.')

    def _update_brokers(self, broker_metadata):
        """Update brokers with fresh metadata."""
        # Remove old brokers
        removed = set(self.brokers.keys()) - set(broker_metadata.keys())
        for id_ in removed:
            logger.info('Removing broker %s', self.brokers[id_])
            self.brokers.pop(id_)
        # Add/update current brokers
        for id_,meta in broker_metadata.iteritems():
            if id_ not in self.brokers:
                self.brokers[id_] = Broker(meta.id, meta.host, meta.port,
                                           self.handler, self._timeout)
                logger.info('Adding new broker %s', self.brokers[id_])
            else:
                broker = self.brokers[id_]
                if meta.host == broker.host and meta.port == broker.port:
                    continue # no changes
                if broker.connected:
                    broker.disconnect()
                logger.info('Updating broker %s', broker)
                broker.host = meta.host
                broker.port = meta.port
                logger.info('Updated broker to %s', broker)

    def _update_topics(self, topic_metadata):
        """Update topics with fresh metadata."""
        # Remove old topics
        removed = set(self.topics.keys()) - set(topic_metadata.keys())
        for name in removed:
            logger.info('Removing topic %s', self.topic[name])
            self.topics.pop(name)
        # Add/update partition information
        for name,meta in topic_metadata.iteritems():
            if name not in self.topics:
                self.topics[name] = Topic(name)
                logger.info('Adding topic %s', self.topics[name])
            # Partitions always need to be updated, even on add
            self._update_partitions(self.topics[name], meta.partitions)

    def _update_partitions(self, topic, partition_metadata):
        """Update partitions with fresh metadata."""
        # Remove old partitions
        removed = set(topic.partitions.keys()) - set(partition_metadata.keys())
        for id_ in removed:
            logger.info('Removing partiton %s', topic.partitons[id_])
            self.brokers.pop(id_)
        # Make sure any brokers referenced are known
        all_brokers = itertools.chain.from_iterable(
            [meta.leader,]+meta.isr+meta.replicas
            for meta in partition_metadata.itervalues()
        )
        if any(b not in self.brokers for b in all_brokers):
            raise Exception('TODO: Type this exception')
        # Add/update current partitions
        for id_,meta in partition_metadata.iteritems():
            if meta.id not in topic.partitions:
                topic.partitions[meta.id] = Partition(
                    topic, meta.id, self.brokers[meta.leader],
                    [self.brokers[b] for b in meta.replicas],
                    [self.brokers[b] for b in meta.isr]
                )
                logger.info('Adding partition %s', topic.partitions[meta.id])
            partition = topic.partitions[id_]
            # Check leader
            if meta.leader != partition.leader.id:
                logger.info('Updating leader for %s', partition)
                partition.leader = self.brokers[meta.leader]
            # Check replica and In-Sync-Replicas lists
            if sorted(r.id for r in partition.replicas) != sorted(meta.replicas):
                logger.info('Updating replicas list for %s', partition)
                partition.replicas = [self.brokers[b] for b in meta.replicas]
            if sorted(i.id for i in partition.isr) != sorted(meta.isr):
                logger.info('Updating in sync replicas list for %s', partition)
                partition.isr = [self.brokers[b] for b in meta.isr]

    def update(self):
        """Update known brokers and topics."""
        metadata = self._get_metadata()
        self._update_brokers(metadata.brokers)
        self._update_topics(metadata.topics)
        # N.B.: Partitions are updated as part of Topic updates.
