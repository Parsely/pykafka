from __future__ import division
import itertools
import logging as log
import socket
import time
import weakref
from uuid import uuid4

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeException, NodeExistsError
from kazoo.recipe.watchers import ChildrenWatch

from kafka.common import OffsetType
from kafka.pykafka.simpleconsumer import SimpleConsumer


class BalancedConsumer():
    def __init__(self,
                 topic,
                 cluster,
                 consumer_group,
                 fetch_message_max_bytes=1024 * 1024,
                 num_consumer_fetchers=1,
                 auto_commit_enable=False,
                 auto_commit_interval_ms=60 * 1000,
                 queued_max_messages=2000,
                 fetch_min_bytes=1,
                 fetch_wait_max_ms=100,
                 refresh_leader_backoff_ms=200,
                 offsets_channel_backoff_ms=1000,
                 offsets_commit_max_retries=5,
                 auto_offset_reset=OffsetType.LATEST,
                 consumer_timeout_ms=-1,
                 rebalance_max_retries=5,
                 rebalance_backoff_ms=2 * 1000,
                 zookeeper_connection_timeout_ms=6 * 1000,
                 zookeeper_connect='127.0.0.1:2181'):
        """Create a BalancedConsumer

        Maintains a single instance of SimpleConsumer, periodically using the
        consumer rebalancing algorithm to reassign partitions to this
        SimpleConsumer.

        :param topic: the topic this consumer should consume
        :type topic: pykafka.topic.Topic
        :param cluster: the cluster this consumer should connect to
        :type cluster: pykafka.cluster.Cluster
        :param consumer_group: the name of the consumer group to join
        :type consumer_group: str
        :param fetch_message_max_bytes: the number of bytes of messages to
            attempt to fetch
        :type fetch_message_max_bytes: int
        :param num_consumer_fetchers: the number of threads used to fetch data
        :type num_consumer_fetchers: int
        :param auto_commit_enable: if true, periodically commit to kafka the
            offset of messages already fetched by this consumer
        :type auto_commit_enable: bool
        :param auto_commit_interval_ms: the frequency in ms that the consumer
            offsets are committed to kafka
        :type auto_commit_interval_ms: int
        :param queued_max_messages: max number of messages buffered for
            consumption
        :type queued_max_messages: int
        :param fetch_min_bytes: the minimum amount of data the server should
            return for a fetch request. If insufficient data is available the
            request will block
        :type fetch_min_bytes: int
        :param fetch_wait_max_ms: the maximum amount of time the server will
            block before answering the fetch request if there isn't sufficient
            data to immediately satisfy fetch_min_bytes
        :type fetch_wait_max_ms: int
        :param refresh_leader_backoff_ms: backoff time to refresh the leader of
            a partition after it loses the current leader
        :type refresh_leader_backoff_ms: int
        :param offsets_channel_backoff_ms: backoff time to retry offset
            commits/fetches
        :type offsets_channel_backoff_ms: int
        :param offsets_commit_max_retries: Retry the offset commit up to this
            many times on failure.
        :type offsets_commit_max_retries: int
        :param auto_offset_reset: what to do if an offset is out of range
        :type auto_offset_reset: int
        :param consumer_timeout_ms: throw a timeout exception to the consumer
            if no message is available for consumption after the specified
            interval
        :type consumer_timeout_ms: int
        :param rebalance_max_retries: Maximum number of attempts before failing to
            rebalance
        :type rebalance_max_retries: int
        :param rebalance_backoff_ms: Backoff time between retries during rebalance.
        :type rebalance_backoff_ms: int
        :param zookeeper_connection_timeout_ms: The max time that the client
            waits while establishing a connection to zookeeper.
        :type zookeeper_connection_timeout_ms: int
        :param zookeeper_connect: comma separated ip:port strings of the
            zookeeper nodes to connect to
        :type zookeeper_connect: str
        """
        if not isinstance(cluster, weakref.ProxyType):
            self._cluster = weakref.proxy(cluster)
        else:
            self._cluster = cluster
        self._consumer_group = consumer_group
        self._topic = topic

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._fetch_message_max_bytes = fetch_message_max_bytes
        self._fetch_min_bytes = fetch_min_bytes
        self._rebalance_max_retries = rebalance_max_retries
        self._num_consumer_fetchers = num_consumer_fetchers
        self._queued_max_messages = queued_max_messages
        self._fetch_wait_max_ms = fetch_wait_max_ms
        self._rebalance_backoff_ms = rebalance_backoff_ms
        self._consumer_timeout_ms = consumer_timeout_ms

        self._consumer = None
        self._consumer_id = "{}:{}".format(socket.gethostname(), uuid4())
        self._partitions = set()
        self._setting_watches = True

        self._topic_path = '/consumers/{}/owners/{}'.format(self._consumer_group,
                                                            self._topic.name)
        self._consumer_id_path = '/consumers/{}/ids'.format(self._consumer_group)

        self._zookeeper = self._setup_zookeeper(zookeeper_connect,
                                                zookeeper_connection_timeout_ms)
        self._zookeeper.ensure_path(self._topic_path)
        self._add_self()
        self._set_watches()
        self._rebalance()

    def stop(self):
        self._zookeeper.stop()
        self._consumer.stop()

    def _setup_zookeeper(self, zookeeper_connect, timeout):
        """Open a connection to a ZooKeeper host

        :param zookeeper_connect: the '<ip>:<port>' address of the zookeeper node to
            which to connect
        :type zookeeper_connect: str
        :param timeout: connection timeout in milliseconds
        :type timeout: int
        """
        zk = KazooClient(zookeeper_connect, timeout=timeout / 1000)
        zk.start()
        return zk

    def _setup_internal_consumer(self):
        """Create an internal SimpleConsumer instance

        If there is already a SimpleConsumer instance held by this object,
        disable its workers and mark it for garbage collection.
        """
        if self._consumer is not None:
            self._consumer.stop()
        self._consumer = SimpleConsumer(
            self._topic, self._cluster,
            consumer_group=self._consumer_group,
            partitions=list(self._partitions),
            auto_commit_enable=self._auto_commit_enable,
            auto_commit_interval_ms=self._auto_commit_interval_ms,
            fetch_message_max_bytes=self._fetch_message_max_bytes,
            fetch_min_bytes=self._fetch_min_bytes,
            num_consumer_fetchers=self._num_consumer_fetchers,
            queued_max_messages=self._queued_max_messages,
            fetch_wait_max_ms=self._fetch_wait_max_ms,
            consumer_timeout_ms=self._consumer_timeout_ms
        )

    def _decide_partitions(self, participants):
        """Decide which partitions belong to this consumer

        Uses the consumer rebalancing algorithm described here
        http://kafka.apache.org/documentation.html

        It is very important that the participants array is sorted,
        since this algorithm runs on each consumer and indexes into the same
        array.

        :param participants: sorted list of ids of the other consumers in this
            consumer group
        :type participants: list
        """
        # Freeze and sort partitions so we always have the same results
        p_to_str = lambda p: '-'.join([p.topic.name, str(p.leader.id), str(p.id)])
        all_partitions = self._topic.partitions.values()
        all_partitions.sort(key=p_to_str)

        # get start point, # of partitions, and remainder
        idx = participants.index(self._consumer_id)
        parts_per_consumer = len(all_partitions) / len(participants)
        remainder_ppc = len(all_partitions) % len(participants)

        start = parts_per_consumer * idx + min(idx, remainder_ppc)
        num_parts = parts_per_consumer + (0 if (idx + 1 > remainder_ppc) else 1)

        # assign partitions from i*N to (i+1)*N - 1 to consumer Ci
        new_partitions = itertools.islice(
            all_partitions,
            start,
            start + num_parts
        )
        new_partitions = set(new_partitions)
        log.info(
            'Balancing %i participants for %i partitions. '
            'My Partitions: %s -- Consumers: %s --- All Partitions: %s',
            len(participants), len(all_partitions),
            [p_to_str(p) for p in new_partitions],
            str(participants),
            [p_to_str(p) for p in all_partitions]
        )
        return new_partitions

    def _get_participants(self):
        """Use zookeeper to get the other consumers of this topic

        Returns a sorted list of the ids of the other consumers of self._topic
        """
        try:
            consumer_ids = self._zookeeper.get_children(self._consumer_id_path)
        except NoNodeException:
            log.debug("Consumer group doesn't exist. "
                      "No participants to find")
            return []

        participants = []
        for id_ in consumer_ids:
            try:
                topic, stat = self._zookeeper.get("%s/%s" % (self._consumer_id_path, id_))
                if topic == self._topic.name:
                    participants.append(id_)
            except NoNodeException:
                pass  # disappeared between ``get_children`` and ``get``
        participants.sort()
        return participants

    def _set_watches(self):
        """Set watches in zookeeper that will trigger rebalances

        Rebalances should be triggered whenever a broker, topic, or consumer
        znode is changed in ZooKeeper.
        """
        self._setting_watches = True
        # Set all our watches and then rebalance
        broker_path = '/brokers/ids'
        try:
            self._broker_watcher = ChildrenWatch(
                self._zookeeper, broker_path,
                self._brokers_changed
            )
        except NoNodeException:
            raise Exception(
                'The broker_path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?'
                % broker_path)

        self._topics_watcher = ChildrenWatch(
            self._zookeeper,
            '/brokers/topics',
            self._topics_changed
        )

        self._consumer_watcher = ChildrenWatch(
            self._zookeeper, self._consumer_id_path,
            self._consumers_changed
        )
        self._setting_watches = False

    def _add_self(self):
        """Register this consumer in zookeeper

        Ensures we don't add more participants than partitions
        """
        participants = self._get_participants()
        if len(self._topic.partitions) <= len(participants):
            log.debug("More consumers than partitions.")
            return

        path = '{}/{}'.format(self._consumer_id_path, self._consumer_id)
        self._zookeeper.create(
            path, self._topic.name, ephemeral=True, makepath=True)

    def _rebalance(self):
        """Join a consumer group and claim partitions.

        Called whenever a ZooKeeper watch is triggered
        """
        log.info('Rebalancing consumer %s for topic %s.' % (
            self._consumer_id, self._topic.name)
        )

        for i in xrange(self._rebalance_max_retries):
            participants = self._get_participants()
            new_partitions = self._decide_partitions(participants)

            self._remove_partitions(self._partitions - new_partitions)

            try:
                self._add_partitions(new_partitions - self._partitions)
                break
            except NodeExistsError:
                log.debug("Partition still owned")

            log.debug("Retrying")
            time.sleep(i * (self._rebalance_backoff_ms / 1000))

        self._setup_internal_consumer()

    def _path_from_partition(self, p):
        return "%s/%s-%s" % (self._topic_path, p.leader.id, p.id)

    def _remove_partitions(self, partitions):
        """Remove partitions from the ZK registry.

        :param partitions: partitions to remove.
        :type partitions: iterable of :class:kafka.pykafka.partition.Partition
        """
        for p in partitions:
            assert p in self._partitions
            self._zookeeper.delete(self._path_from_partition(p))
        self._partitions -= partitions

    def _add_partitions(self, partitions):
        """Add partitions to the ZK registry.

        :param partitions: partitions to add.
        :type partitions: iterable of :class:kafka.pykafka.partition.Partition
        """
        for p in partitions:
            self._zookeeper.create(
                self._path_from_partition(p), self._consumer_id,
                ephemeral=True
            )
        self._partitions |= partitions

    def _brokers_changed(self, brokers):
        if self._setting_watches:
            return
        log.debug("Rebalance triggered by broker change")
        self._rebalance()

    def _consumers_changed(self, consumers):
        if self._setting_watches:
            return
        log.debug("Rebalance triggered by consumer change")
        self._rebalance()

    def _topics_changed(self, topics):
        if self._setting_watches:
            return
        log.debug("Rebalance triggered by topic change")
        self._rebalance()

    def consume(self):
        """Get one message from the consumer"""
        return self._consumer.consume()

    def __iter__(self):
        while True:
            yield self._consumer.consume()
