import logging

from .broker import Broker
from .topic import Topic


logger = logging.getLogger(__name__)


class Cluster(object):
    """Cluster implementation used to populate the SamsaClient."""

    def __init__(self, hosts, handler, timeout):
        self._seed_hosts = hosts
        self._timeout = timeout
        self._handler = handler
        self._brokers = {}
        self._topics = {}
        self.update()

    @property
    def brokers(self):
        return self._brokers

    @property
    def topics(self):
        return self._topics

    def _get_metadata(self):
        """Get fresh cluster metadata from a broker"""
        # Works either on existing brokers or seed_hosts list
        if self.brokers:
            brokers = self.brokers.values()
        else:
            brokers = self._seed_hosts.split(',')

        for broker in brokers:
            try:
                if isinstance(broker, basestring):
                    h, p = broker.split(':')
                    broker = Broker(-1, h, p, self._handler, self._timeout)
                return broker.request_metadata()
            # TODO: Change to typed exception
            except Exception:
                logger.exception('Unable to connect to broker %s:%s',
                                 broker.host,
                                 broker.port)
        raise Exception('Unable to connect to a broker to fetch metadata.')

    def _update_brokers(self, broker_metadata):
        """Update brokers with fresh metadata.

        :param broker_metadata: Metadata for all brokers
        :type broker_metadata: Dict of `{name: metadata}` where `metadata is
            :class:`samsa.pysamsa.protocol.BrokerMetadata`
        """
        # Remove old brokers
        removed = set(self._brokers.keys()) - set(broker_metadata.keys())
        for id_ in removed:
            logger.info('Removing broker %s', self._brokers[id_])
            self._brokers.pop(id_)
        # Add/update current brokers
        for id_, meta in broker_metadata.iteritems():
            if id_ not in self._brokers:
                logger.info('Adding new broker %s:%s', meta.host, meta.port)
                self._brokers[id_] = Broker(meta, self._handler, self._timeout)
            else:
                broker = self._brokers[id_]
                if meta.host == broker.host and meta.port == broker.port:
                    continue  # no changes
                # TODO: Can brokers update? Seems like a problem if so.
                #       Figure out and implement update/disconnect/reconnect if
                #       needed.
                raise Exception('Broker host/port change detected! %s', broker)

    def _update_topics(self, metadata):
        """Update topics with fresh metadata.

        :param metadata: Metadata for all topics
        :type metadata: Dict of `{name, metadata}` where `metadata` is
            :class:`samsa.pysamsa.protocol.TopicMetadata`
        """
        # Remove old topics
        removed = set(self._topics.keys()) - set(metadata.keys())
        for name in removed:
            logger.info('Removing topic %s', self._topics[name])
            self._topics.pop(name)
        # Add/update partition information
        for name, meta in metadata.iteritems():
            if name not in self._topics:
                self._topics[name] = Topic(self._brokers, meta)
                logger.info('Adding topic %s', self._topics[name])
            else:
                self._topics[name].update(meta)

    def update(self):
        """Update known brokers and topics."""
        metadata = self._get_metadata()
        self._update_brokers(metadata.brokers)
        self._update_topics(metadata.topics)
        # N.B.: Partitions are updated as part of Topic updates.
