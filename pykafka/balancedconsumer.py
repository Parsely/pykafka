from __future__ import division
"""
Author: Emmett Butler
"""
__license__ = """
Copyright 2015 Parse.ly, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
__all__ = ["BalancedConsumer"]
import itertools
import logging
import socket
import time
from uuid import uuid4

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeException, NodeExistsError
from kazoo.recipe.watchers import ChildrenWatch

from .common import OffsetType
from .exceptions import (KafkaException, PartitionOwnedError,
                         ConsumerStoppedException)
from .simpleconsumer import SimpleConsumer
from .utils.compat import range, get_bytes


log = logging.getLogger(__name__)


class BalancedConsumer():
    """
    A self-balancing consumer for Kafka that uses ZooKeeper to communicate
    with other balancing consumers.

    Maintains a single instance of SimpleConsumer, periodically using the
    consumer rebalancing algorithm to reassign partitions to this
    SimpleConsumer.
    """
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
                 offsets_channel_backoff_ms=1000,
                 offsets_commit_max_retries=5,
                 auto_offset_reset=OffsetType.EARLIEST,
                 consumer_timeout_ms=-1,
                 rebalance_max_retries=5,
                 rebalance_backoff_ms=2 * 1000,
                 zookeeper_connection_timeout_ms=6 * 1000,
                 zookeeper_connect='127.0.0.1:2181',
                 zookeeper=None,
                 auto_start=True,
                 reset_offset_on_start=False):
        """Create a BalancedConsumer instance

        :param topic: The topic this consumer should consume
        :type topic: :class:`pykafka.topic.Topic`
        :param cluster: The cluster to which this consumer should connect
        :type cluster: :class:`pykafka.cluster.Cluster`
        :param consumer_group: The name of the consumer group this consumer
            should join.
        :type consumer_group: bytes
        :param fetch_message_max_bytes: The number of bytes of messages to
            attempt to fetch with each fetch request
        :type fetch_message_max_bytes: int
        :param num_consumer_fetchers: The number of workers used to make
            FetchRequests
        :type num_consumer_fetchers: int
        :param auto_commit_enable: If true, periodically commit to kafka the
            offset of messages already fetched by this consumer. This also
            requires that `consumer_group` is not `None`.
        :type auto_commit_enable: bool
        :param auto_commit_interval_ms: The frequency (in milliseconds) at which
            the consumer's offsets are committed to kafka. This setting is
            ignored if `auto_commit_enable` is `False`.
        :type auto_commit_interval_ms: int
        :param queued_max_messages: The maximum number of messages buffered for
            consumption in the internal
            :class:`pykafka.simpleconsumer.SimpleConsumer`
        :type queued_max_messages: int
        :param fetch_min_bytes: The minimum amount of data (in bytes) that the
            server should return for a fetch request. If insufficient data is
            available, the request will block until sufficient data is available.
        :type fetch_min_bytes: int
        :param fetch_wait_max_ms: The maximum amount of time (in milliseconds)
            that the server will block before answering a fetch request if
            there isn't sufficient data to immediately satisfy `fetch_min_bytes`.
        :type fetch_wait_max_ms: int
        :param offsets_channel_backoff_ms: Backoff time to retry failed offset
            commits and fetches.
        :type offsets_channel_backoff_ms: int
        :param offsets_commit_max_retries: The number of times the offset commit
            worker should retry before raising an error.
        :type offsets_commit_max_retries: int
        :param auto_offset_reset: What to do if an offset is out of range. This
            setting indicates how to reset the consumer's internal offset
            counter when an `OffsetOutOfRangeError` is encountered.
        :type auto_offset_reset: :class:`pykafka.common.OffsetType`
        :param consumer_timeout_ms: Amount of time (in milliseconds) the
            consumer may spend without messages available for consumption
            before returning None.
        :type consumer_timeout_ms: int
        :param rebalance_max_retries: The number of times the rebalance should
            retry before raising an error.
        :type rebalance_max_retries: int
        :param rebalance_backoff_ms: Backoff time (in milliseconds) between
            retries during rebalance.
        :type rebalance_backoff_ms: int
        :param zookeeper_connection_timeout_ms: The maximum time (in
            milliseconds) that the consumer waits while establishing a
            connection to zookeeper.
        :type zookeeper_connection_timeout_ms: int
        :param zookeeper_connect: Comma-separated (ip1:port1,ip2:port2) strings
            indicating the zookeeper nodes to which to connect.
        :type zookeeper_connect: str
        :param zookeeper: A KazooClient connected to a Zookeeper instance.
            If provided, `zookeeper_connect` is ignored.
        :type zookeeper: :class:`kazoo.client.KazooClient`
        :param auto_start: Whether the consumer should begin communicating
            with zookeeper after __init__ is complete. If false, communication
            can be started with `start()`.
        :type auto_start: bool
        :param reset_offset_on_start: Whether the consumer should reset its
            internal offset counter to `self._auto_offset_reset` and commit that
            offset immediately upon starting up
        :type reset_offset_on_start: bool
        """
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
        self._offsets_channel_backoff_ms = offsets_channel_backoff_ms
        self._offsets_commit_max_retries = offsets_commit_max_retries
        self._auto_offset_reset = auto_offset_reset
        self._zookeeper_connect = zookeeper_connect
        self._zookeeper_connection_timeout_ms = zookeeper_connection_timeout_ms
        self._reset_offset_on_start = reset_offset_on_start
        self._running = False

        self._rebalancing_lock = cluster.handler.Lock()
        self._consumer = None
        self._consumer_id = "{hostname}:{uuid}".format(
            hostname=socket.gethostname(),
            uuid=uuid4()
        )
        self._partitions = set()
        self._setting_watches = True

        self._topic_path = '/consumers/{group}/owners/{topic}'.format(
            group=self._consumer_group,
            topic=self._topic.name)
        self._consumer_id_path = '/consumers/{group}/ids'.format(
            group=self._consumer_group)

        self._zookeeper = None
        if zookeeper is not None:
            self._zookeeper = zookeeper
        if auto_start is True:
            self.start()

    def __repr__(self):
        return "<{module}.{name} at {id_} (consumer_group={group})>".format(
            module=self.__class__.__module__,
            name=self.__class__.__name__,
            id_=hex(id(self)),
            group=self._consumer_group
        )

    def _setup_checker_worker(self):
        """Start the zookeeper partition checker thread"""
        def checker():
            while True:
                time.sleep(120)
                if not self._running:
                    break
                self._check_held_partitions()
            log.debug("Checker thread exiting")
        log.debug("Starting checker thread")
        return self._cluster.handler.spawn(checker)

    @property
    def partitions(self):
        return self._consumer.partitions if self._consumer else None

    @property
    def held_offsets(self):
        """Return a map from partition id to held offset for each partition"""
        if not self._consumer:
            return None
        return dict((p.partition.id, p.last_offset_consumed)
                    for p in self._consumer._partitions_by_id.itervalues())

    def start(self):
        """Open connections and join a cluster."""
        if self._zookeeper is None:
            self._setup_zookeeper(self._zookeeper_connect,
                                  self._zookeeper_connection_timeout_ms)
        self._zookeeper.ensure_path(self._topic_path)
        self._add_self()
        self._set_watches()
        self._rebalance()
        self._running = True
        self._setup_checker_worker()

    def stop(self):
        """Close the zookeeper connection and stop consuming.

        This method should be called as part of a graceful shutdown process.
        """
        self._zookeeper.stop()
        self._consumer.stop()
        self._running = False

    def _setup_zookeeper(self, zookeeper_connect, timeout):
        """Open a connection to a ZooKeeper host.

        :param zookeeper_connect: The 'ip:port' address of the zookeeper node to
            which to connect.
        :type zookeeper_connect: str
        :param timeout: Connection timeout (in milliseconds)
        :type timeout: int
        """
        self._zookeeper = KazooClient(zookeeper_connect, timeout=timeout / 1000)
        self._zookeeper.start()

    def _setup_internal_consumer(self, start=True):
        """Instantiate an internal SimpleConsumer.

        If there is already a SimpleConsumer instance held by this object,
        disable its workers and mark it for garbage collection before
        creating a new one.
        """
        reset_offset_on_start = self._reset_offset_on_start
        if self._consumer is not None:
            self._consumer.stop()
            # only use this setting for the first call to
            # _setup_internal_consumer. subsequent calls should not
            # reset the offsets, since they can happen at any time
            reset_offset_on_start = False
        self._consumer = SimpleConsumer(
            self._topic,
            self._cluster,
            consumer_group=self._consumer_group,
            partitions=list(self._partitions),
            auto_commit_enable=self._auto_commit_enable,
            auto_commit_interval_ms=self._auto_commit_interval_ms,
            fetch_message_max_bytes=self._fetch_message_max_bytes,
            fetch_min_bytes=self._fetch_min_bytes,
            num_consumer_fetchers=self._num_consumer_fetchers,
            queued_max_messages=self._queued_max_messages,
            fetch_wait_max_ms=self._fetch_wait_max_ms,
            consumer_timeout_ms=self._consumer_timeout_ms,
            offsets_channel_backoff_ms=self._offsets_channel_backoff_ms,
            offsets_commit_max_retries=self._offsets_commit_max_retries,
            auto_offset_reset=self._auto_offset_reset,
            reset_offset_on_start=reset_offset_on_start,
            auto_start=start
        )

    def _decide_partitions(self, participants):
        """Decide which partitions belong to this consumer.

        Uses the consumer rebalancing algorithm described here
        http://kafka.apache.org/documentation.html

        It is very important that the participants array is sorted,
        since this algorithm runs on each consumer and indexes into the same
        array. The same array index operation must return the same
        result on each consumer.

        :param participants: Sorted list of ids of all other consumers in this
            consumer group.
        :type participants: Iterable of `bytes`
        """
        # Freeze and sort partitions so we always have the same results
        p_to_str = lambda p: '-'.join([str(p.topic.name), str(p.leader.id), str(p.id)])
        all_parts = self._topic.partitions.values()
        all_parts = sorted(all_parts, key=p_to_str)

        # get start point, # of partitions, and remainder
        participants = sorted(participants)  # just make sure it's sorted.
        idx = participants.index(self._consumer_id)
        parts_per_consumer = len(all_parts) // len(participants)
        remainder_ppc = len(all_parts) % len(participants)

        start = parts_per_consumer * idx + min(idx, remainder_ppc)
        num_parts = parts_per_consumer + (0 if (idx + 1 > remainder_ppc) else 1)

        # assign partitions from i*N to (i+1)*N - 1 to consumer Ci
        new_partitions = itertools.islice(all_parts, start, start + num_parts)
        new_partitions = set(new_partitions)
        log.info('Balancing %i participants for %i partitions.\nOwning %i partitions.',
                 len(participants), len(all_parts), len(new_partitions))
        log.debug('My partitions: %s', [p_to_str(p) for p in new_partitions])
        return new_partitions

    def _get_participants(self):
        """Use zookeeper to get the other consumers of this topic.

        :return: A sorted list of the ids of the other consumers of this
            consumer's topic
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
        participants = sorted(participants)
        return participants

    def _set_watches(self):
        """Set watches in zookeeper that will trigger rebalances.

        Rebalances should be triggered whenever a broker, topic, or consumer
        znode is changed in zookeeper. This ensures that the balance of the
        consumer group remains up-to-date with the current state of the
        cluster.
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
        """Register this consumer in zookeeper.

        This method ensures that the number of participants is at most the
        number of partitions.
        """
        participants = self._get_participants()
        if len(self._topic.partitions) <= len(participants):
            raise KafkaException("Cannot add consumer: more consumers than partitions")

        path = '{path}/{id_}'.format(
            path=self._consumer_id_path,
            id_=self._consumer_id
        )
        self._zookeeper.create(
            path, self._topic.name, ephemeral=True, makepath=True)

    def _rebalance(self):
        """Claim partitions for this consumer.

        This method is called whenever a zookeeper watch is triggered.
        """
        if self._consumer is not None:
            self.commit_offsets()
        with self._rebalancing_lock:
            log.info('Rebalancing consumer %s for topic %s.' % (
                self._consumer_id, self._topic.name)
            )

            for i in range(self._rebalance_max_retries):
                try:
                    # If retrying, be sure to make sure the
                    # partition allocation is correct.
                    participants = self._get_participants()
                    partitions = self._decide_partitions(participants)

                    old_partitions = self._partitions - partitions
                    self._remove_partitions(old_partitions)

                    new_partitions = partitions - self._partitions
                    self._add_partitions(new_partitions)

                    # Only re-create internal consumer if something changed.
                    if old_partitions or new_partitions:
                        self._setup_internal_consumer()

                    log.info('Rebalancing Complete.')
                    break
                except PartitionOwnedError as ex:
                    if i == self._rebalance_max_retries - 1:
                        log.warning('Failed to acquire partition %s after %d retries.',
                                    ex.partition, i)
                        raise
                    log.info('Unable to acquire partition %s. Retrying', ex.partition)
                    time.sleep(i * (self._rebalance_backoff_ms / 1000))

    def _path_from_partition(self, p):
        """Given a partition, return its path in zookeeper.

        :type p: :class:`pykafka.partition.Partition`
        """
        return "%s/%s-%s" % (self._topic_path, p.leader.id, p.id)

    def _remove_partitions(self, partitions):
        """Remove partitions from the zookeeper registry for this consumer.

        Also remove these partitions from the consumer's internal
        partition registry.

        :param partitions: The partitions to remove.
        :type partitions: Iterable of :class:`pykafka.partition.Partition`
        """
        for p in partitions:
            assert p in self._partitions
            self._zookeeper.delete(self._path_from_partition(p))
        self._partitions -= partitions

    def _add_partitions(self, partitions):
        """Add partitions to the zookeeper registry for this consumer.

        Also add these partitions to the consumer's internal partition registry.

        :param partitions: The partitions to add.
        :type partitions: Iterable of :class:`pykafka.partition.Partition`
        """
        for p in partitions:
            try:
                self._zookeeper.create(
                    self._path_from_partition(p),
                    value=get_bytes(self._consumer_id),
                    ephemeral=True
                )
                self._partitions.add(p)
            except NodeExistsError:
                raise PartitionOwnedError(p)

    def _check_held_partitions(self):
        """Double-check held partitions against zookeeper

        Ensure that the partitions held by this consumer are the ones that
        zookeeper thinks it's holding. If not, rebalance.
        """
        log.info("Checking held partitions against ZooKeeper")
        # build a set of partition ids zookeeper says we own
        zk_partition_ids = set()
        all_partitions = self._zookeeper.get_children(self._topic_path)
        for partition_slug in all_partitions:
            owner_id, stat = self._zookeeper.get(
                '{path}/{slug}'.format(
                    path=self._topic_path, slug=partition_slug))
            if owner_id == self._consumer_id:
                zk_partition_ids.add(int(partition_slug.split('-')[1]))
        # build a set of partition ids we think we own
        internal_partition_ids = set([p.id for p in self._partitions])
        # compare the two sets, rebalance if necessary
        if internal_partition_ids != zk_partition_ids:
            log.warning("Internal partition registry doesn't match ZooKeeper!")
            log.debug("Internal partition ids: %s\nZooKeeper partition ids: %s",
                      internal_partition_ids, zk_partition_ids)
            self._rebalance()

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

    def reset_offsets(self, partition_offsets=None):
        """Reset offsets for the specified partitions

        Issue an OffsetRequest for each partition and set the appropriate
        returned offset in the OwnedPartition

        :param partition_offsets: (`partition`, `offset`) pairs to reset
            where `partition` is the partition for which to reset the offset
            and `offset` is the new offset the partition should have
        :type partition_offsets: Iterable of
            (:class:`pykafka.partition.Partition`, int)
        """
        if not self._consumer:
            raise ConsumerStoppedException("Internal consumer is stopped")
        self._consumer.reset_offsets(partition_offsets=partition_offsets)

    def consume(self, block=True):
        """Get one message from the consumer

        :param block: Whether to block while waiting for a message
        :type block: bool
        """

        def consumer_timed_out():
            """Indicates whether the consumer has received messages recently"""
            if self._consumer_timeout_ms == -1:
                return False
            disp = (time.time() - self._last_message_time) * 1000.0
            return disp > self._consumer_timeout_ms
        message = None
        self._last_message_time = time.time()
        while message is None and not consumer_timed_out():
            try:
                message = self._consumer.consume(block=block)
            except ConsumerStoppedException:
                if not self._running:
                    return
                continue
            if message:
                self._last_message_time = time.time()
            if not block:
                return message
        return message

    def commit_offsets(self):
        """Commit offsets for this consumer's partitions

        Uses the offset commit/fetch API
        """
        return self._consumer.commit_offsets()
