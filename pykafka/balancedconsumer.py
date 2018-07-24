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
import logging
import socket
import sys
import time
from uuid import uuid4
import weakref

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeException, NodeExistsError
from kazoo.recipe.watchers import ChildrenWatch
try:
    from kazoo.handlers.gevent import SequentialGeventHandler
except ImportError:
    SequentialGeventHandler = None
from six import reraise

from .common import OffsetType
from .exceptions import (KafkaException, PartitionOwnedError, ConsumerStoppedException,
                         UnicodeException)
from .membershipprotocol import RangeProtocol
from .simpleconsumer import SimpleConsumer
from .utils.compat import range, get_bytes, itervalues, iteritems, get_string
from .utils.error_handlers import valid_int
try:
    from .handlers import GEventHandler
except ImportError:
    GEventHandler = None
try:
    from . import rdkafka
except ImportError:
    rdkafka = False


log = logging.getLogger(__name__)


def _catch_thread_exception(fn):
    """Sets self._worker_exception when fn raises an exception"""
    def wrapped(self, *args, **kwargs):
        try:
            ret = fn(self, *args, **kwargs)
        except Exception:
            self._worker_exception = sys.exc_info()
        else:
            return ret
    return wrapped


class BalancedConsumer(object):
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
                 fetch_error_backoff_ms=500,
                 fetch_wait_max_ms=100,
                 offsets_channel_backoff_ms=1000,
                 offsets_commit_max_retries=5,
                 auto_offset_reset=OffsetType.EARLIEST,
                 consumer_timeout_ms=-1,
                 rebalance_max_retries=5,
                 rebalance_backoff_ms=2 * 1000,
                 zookeeper_connection_timeout_ms=6 * 1000,
                 zookeeper_connect=None,
                 zookeeper_hosts='127.0.0.1:2181',
                 zookeeper=None,
                 auto_start=True,
                 reset_offset_on_start=False,
                 post_rebalance_callback=None,
                 use_rdkafka=False,
                 compacted_topic=False,
                 membership_protocol=RangeProtocol,
                 deserializer=None,
                 reset_offset_on_fetch=True):
        """Create a BalancedConsumer instance

        :param topic: The topic this consumer should consume
        :type topic: :class:`pykafka.topic.Topic`
        :param cluster: The cluster to which this consumer should connect
        :type cluster: :class:`pykafka.cluster.Cluster`
        :param consumer_group: The name of the consumer group this consumer
            should join. Consumer group names are namespaced at the cluster level,
            meaning that two consumers consuming different topics with the same group name
            will be treated as part of the same group.
        :type consumer_group: str
        :param fetch_message_max_bytes: The number of bytes of messages to
            attempt to fetch with each fetch request
        :type fetch_message_max_bytes: int
        :param num_consumer_fetchers: The number of workers used to make
            FetchRequests
        :type num_consumer_fetchers: int
        :param auto_commit_enable: If true, periodically commit to kafka the
            offset of messages already returned from consume() calls. Requires that
            `consumer_group` is not `None`.
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
        :param fetch_error_backoff_ms: *UNUSED*.
            See :class:`pykafka.simpleconsumer.SimpleConsumer`.
        :type fetch_error_backoff_ms: int
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
        :param zookeeper_connect: Deprecated::2.7,3.6 Comma-Separated
            (ip1:port1,ip2:port2) strings indicating the zookeeper nodes to which
            to connect.
        :type zookeeper_connect: str
        :param zookeeper_hosts: KazooClient-formatted string of ZooKeeper hosts to which
            to connect.
        :type zookeeper_hosts: str
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
        :param post_rebalance_callback: A function to be called when a rebalance is
            in progress. This function should accept three arguments: the
            :class:`pykafka.balancedconsumer.BalancedConsumer` instance that just
            completed its rebalance, a dict of partitions that it owned before the
            rebalance, and a dict of partitions it owns after the rebalance. These dicts
            map partition ids to the most recently known offsets for those partitions.
            This function can optionally return a dictionary mapping partition ids to
            offsets. If it does, the consumer will reset its offsets to the supplied
            values before continuing consumption.
            Note that the BalancedConsumer is in a poorly defined state at
            the time this callback runs, so that accessing its properties
            (such as `held_offsets` or `partitions`) might yield confusing
            results.  Instead, the callback should really rely on the
            provided partition-id dicts, which are well-defined.
        :type post_rebalance_callback: function
        :param use_rdkafka: Use librdkafka-backed consumer if available
        :type use_rdkafka: bool
        :param compacted_topic: Set to read from a compacted topic. Forces
            consumer to use less stringent message ordering logic because compacted
            topics do not provide offsets in strict incrementing order.
        :type compacted_topic: bool
        :param membership_protocol: The group membership protocol to which this consumer
            should adhere
        :type membership_protocol: :class:`pykafka.membershipprotocol.GroupMembershipProtocol`
        :param deserializer: A function defining how to deserialize messages returned
            from Kafka. A function with the signature d(value, partition_key) that
            returns a tuple of (deserialized_value, deserialized_partition_key). The
            arguments passed to this function are the bytes representations of a
            message's value and partition key, and the returned data should be these
            fields transformed according to the client code's serialization logic.
            See `pykafka.utils.__init__` for stock implemtations.
        :type deserializer: function
        :param reset_offset_on_fetch: Whether to update offsets during fetch_offsets.
               Disable for read-only use cases to prevent side-effects.
        :type reset_offset_on_fetch: bool
        """
        self._cluster = cluster
        try:
            self._consumer_group = get_string(consumer_group).encode('ascii')
        except UnicodeEncodeError:
            raise UnicodeException("Consumer group name '{}' contains non-ascii "
                                   "characters".format(consumer_group))
        self._topic = topic

        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = valid_int(auto_commit_interval_ms)
        self._fetch_message_max_bytes = valid_int(fetch_message_max_bytes)
        self._fetch_min_bytes = valid_int(fetch_min_bytes)
        self._rebalance_max_retries = valid_int(rebalance_max_retries, allow_zero=True)
        self._num_consumer_fetchers = valid_int(num_consumer_fetchers)
        self._queued_max_messages = valid_int(queued_max_messages)
        self._fetch_wait_max_ms = valid_int(fetch_wait_max_ms, allow_zero=True)
        self._rebalance_backoff_ms = valid_int(rebalance_backoff_ms)
        self._consumer_timeout_ms = valid_int(consumer_timeout_ms,
                                              allow_zero=True, allow_negative=True)
        self._offsets_channel_backoff_ms = valid_int(offsets_channel_backoff_ms)
        self._offsets_commit_max_retries = valid_int(offsets_commit_max_retries,
                                                     allow_zero=True)
        self._auto_offset_reset = auto_offset_reset
        self._zookeeper_connect = zookeeper_connect or zookeeper_hosts
        self._zookeeper_connection_timeout_ms = valid_int(zookeeper_connection_timeout_ms,
                                                          allow_zero=True)
        self._reset_offset_on_start = reset_offset_on_start
        self._post_rebalance_callback = post_rebalance_callback
        self._generation_id = -1
        self._running = False
        self._worker_exception = None
        self._is_compacted_topic = compacted_topic
        self._membership_protocol = membership_protocol
        self._deserializer = deserializer
        self._reset_offset_on_fetch = reset_offset_on_fetch

        if not rdkafka and use_rdkafka:
            raise ImportError("use_rdkafka requires rdkafka to be installed")
        if GEventHandler and isinstance(self._cluster.handler, GEventHandler) and use_rdkafka:
            raise ImportError("use_rdkafka cannot be used with gevent")
        self._use_rdkafka = rdkafka and use_rdkafka

        self._rebalancing_lock = cluster.handler.Lock()
        self._rebalancing_in_progress = self._cluster.handler.Event()
        self._internal_consumer_running = self._cluster.handler.Event()
        self._consumer = None
        self._consumer_id = get_bytes("{hostname}:{uuid}".format(
            hostname=socket.gethostname(),
            uuid=uuid4()
        ))
        self._setting_watches = True

        self._topic_path = '/consumers/{group}/owners/{topic}'.format(
            group=get_string(self._consumer_group),
            topic=self._topic.name)
        self._consumer_id_path = '/consumers/{group}/ids'.format(
            group=get_string(self._consumer_group))

        self._zookeeper = None
        self._owns_zookeeper = zookeeper is None
        if zookeeper is not None:
            self._zookeeper = zookeeper
        if auto_start is True:
            self.start()

    def __del__(self):
        log.debug("Finalising {}".format(self))
        if self._running:
            self.stop()

    def __repr__(self):
        return "<{module}.{name} at {id_} (consumer_group={group})>".format(
            module=self.__class__.__module__,
            name=self.__class__.__name__,
            id_=hex(id(self)),
            group=get_string(self._consumer_group)
        )

    def _raise_worker_exceptions(self):
        """Raises exceptions encountered on worker threads"""
        if self._worker_exception is not None:
            reraise(*self._worker_exception)

    @property
    def topic(self):
        """The topic this consumer consumes"""
        return self._topic

    @property
    def partitions(self):
        """A list of the partitions that this consumer consumes"""
        return self._consumer.partitions if self._consumer else dict()

    @property
    def _partitions(self):
        """Convenient shorthand for set of partitions internally held"""
        return set(
            [] if self.partitions is None else itervalues(self.partitions))

    @property
    def held_offsets(self):
        """Return a map from partition id to held offset for each partition"""
        if not self._consumer:
            return None
        return self._consumer.held_offsets

    def start(self):
        """Open connections and join a consumer group."""
        try:
            if self._zookeeper is None:
                self._setup_zookeeper(self._zookeeper_connect,
                                      self._zookeeper_connection_timeout_ms)
            self._zookeeper.ensure_path(self._topic_path)
            self._add_self()
            self._running = True
            self._set_watches()
            self._rebalance()
        except Exception:
            log.exception("Stopping consumer in response to error")
            self.stop()

    def stop(self):
        """Close the zookeeper connection and stop consuming.

        This method should be called as part of a graceful shutdown process.
        """
        log.debug("Stopping {}".format(self))
        with self._rebalancing_lock:
            # We acquire the lock in order to prevent a race condition where a
            # rebalance that is already underway might re-register the zk
            # nodes that we remove here
            self._running = False
        if self._consumer is not None:
            self._consumer.stop()
        if self._owns_zookeeper:
            # NB this should always come last, so we do not hand over control
            # of our partitions until consumption has really been halted
            self._zookeeper.stop()
        else:
            self._remove_partitions(self._get_held_partitions())
            try:
                self._zookeeper.delete(self._path_self)
            except NoNodeException:
                pass
        # additionally we'd want to remove watches here, but there are no
        # facilities for that in ChildrenWatch - as a workaround we check
        # self._running in the watcher callbacks (see further down)

    def _setup_zookeeper(self, zookeeper_connect, timeout):
        """Open a connection to a ZooKeeper host.

        :param zookeeper_connect: The 'ip:port' address of the zookeeper node to
            which to connect.
        :type zookeeper_connect: str
        :param timeout: Connection timeout (in milliseconds)
        :type timeout: int
        """
        kazoo_kwargs = {'timeout': timeout / 1000}
        if GEventHandler and isinstance(self._cluster.handler, GEventHandler):
            kazoo_kwargs['handler'] = SequentialGeventHandler()
        self._zookeeper = KazooClient(zookeeper_connect, **kazoo_kwargs)
        self._zookeeper.start()

    def _setup_internal_consumer(self, partitions=None, start=True):
        """Instantiate an internal SimpleConsumer instance"""
        if partitions is None:
            partitions = []
        # Only re-create internal consumer if something changed.
        if partitions != self._partitions:
            cns = self._get_internal_consumer(partitions=list(partitions), start=start)
            if self._post_rebalance_callback is not None:
                old_offsets = (self._consumer.held_offsets
                               if self._consumer else dict())
                new_offsets = cns.held_offsets
                try:
                    reset_offsets = self._post_rebalance_callback(
                        self, old_offsets, new_offsets)
                except Exception:
                    log.exception("post rebalance callback threw an exception")
                    self._worker_exception = sys.exc_info()
                    return False

                if reset_offsets:
                    cns.reset_offsets(partition_offsets=[
                        (cns.partitions[id_], offset) for
                        (id_, offset) in iteritems(reset_offsets)])
            self._consumer = cns
        if self._consumer and self._consumer._running:
            if not self._internal_consumer_running.is_set():
                self._internal_consumer_running.set()
        else:
            if self._internal_consumer_running.is_set():
                self._internal_consumer_running.clear()
        return True

    def _get_internal_consumer(self, partitions=None, start=True):
        """Instantiate a SimpleConsumer for internal use.

        If there is already a SimpleConsumer instance held by this object,
        disable its workers and mark it for garbage collection before
        creating a new one.
        """
        if partitions is None:
            partitions = []
        reset_offset_on_start = self._reset_offset_on_start
        if self._consumer is not None:
            self._consumer.stop()
            # only use this setting for the first call to
            # _get_internal_consumer. subsequent calls should not
            # reset the offsets, since they can happen at any time
            reset_offset_on_start = False
        Cls = (rdkafka.RdKafkaSimpleConsumer
               if self._use_rdkafka else SimpleConsumer)
        cns = Cls(
            self._topic,
            self._cluster,
            consumer_group=self._consumer_group,
            partitions=partitions,
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
            auto_start=False,
            compacted_topic=self._is_compacted_topic,
            deserializer=self._deserializer,
            reset_offset_on_fetch=self._reset_offset_on_fetch
        )
        cns.consumer_id = self._consumer_id
        cns.generation_id = self._generation_id
        if start:
            cns.start()
        return cns

    def _get_participants(self):
        """Use zookeeper to get the other consumers of this topic.

        :return: A sorted list of the ids of other consumers of this
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
                    participants.append(get_bytes(id_))
            except NoNodeException:
                pass  # node disappeared between ``get_children`` and ``get``
        participants = sorted(participants)
        return participants

    def _build_watch_callback(self, fn, proxy):
        """Return a function that's safe to use as a ChildrenWatch callback

        Fixes the issue from https://github.com/Parsely/pykafka/issues/345
        """
        def _callback(children):
            # discover whether the referenced object still exists
            try:
                proxy.__repr__()
            except ReferenceError:
                return False
            return fn(proxy, children)
        return _callback

    def _set_watches(self):
        """Set watches in zookeeper that will trigger rebalances.

        Rebalances should be triggered whenever a broker, topic, or consumer
        znode is changed in zookeeper. This ensures that the balance of the
        consumer group remains up-to-date with the current state of the
        cluster.
        """
        proxy = weakref.proxy(self)
        _brokers_changed = self._build_watch_callback(BalancedConsumer._brokers_changed, proxy)
        _topics_changed = self._build_watch_callback(BalancedConsumer._topics_changed, proxy)
        _consumers_changed = self._build_watch_callback(BalancedConsumer._consumers_changed, proxy)

        self._setting_watches = True
        # Set all our watches and then rebalance
        broker_path = '/brokers/ids'
        try:
            self._broker_watcher = ChildrenWatch(
                self._zookeeper, broker_path,
                _brokers_changed
            )
        except NoNodeException:
            raise Exception(
                'The broker_path "%s" does not exist in your '
                'ZooKeeper cluster -- is your Kafka cluster running?'
                % broker_path)

        self._topics_watcher = ChildrenWatch(
            self._zookeeper,
            '/brokers/topics',
            _topics_changed
        )

        self._consumer_watcher = ChildrenWatch(
            self._zookeeper, self._consumer_id_path,
            _consumers_changed
        )
        self._setting_watches = False

    def _add_self(self):
        """Register this consumer in zookeeper."""
        self._zookeeper.create(
            self._path_self, self._topic.name, ephemeral=True, makepath=True)

    @property
    def _path_self(self):
        """Path where this consumer should be registered in zookeeper"""
        return '{path}/{id_}'.format(
            path=self._consumer_id_path,
            # get_string is necessary to avoid writing literal "b'" to zookeeper
            id_=get_string(self._consumer_id)
        )

    def _update_member_assignment(self):
        """Decide and assign new partitions for this consumer"""
        for i in range(self._rebalance_max_retries):
            try:
                # If retrying, be sure to make sure the
                # partition allocation is correct.
                participants = self._get_participants()
                if self._consumer_id not in participants:
                    # situation that only occurs if our zk session expired
                    self._add_self()
                    participants.append(self._consumer_id)

                new_partitions = self._membership_protocol.decide_partitions(
                    participants, self._topic.partitions, self._consumer_id)
                if not new_partitions:
                    log.warning("No partitions assigned to consumer %s",
                                self._consumer_id)

                # Update zk with any changes:
                # Note that we explicitly fetch our set of held partitions
                # from zk, rather than assuming it will be identical to
                # `self.partitions`.  This covers the (rare) situation
                # where due to an interrupted connection our zk session
                # has expired, in which case we'd hold zero partitions on
                # zk, but `self._partitions` may be outdated and non-empty
                current_zk_parts = self._get_held_partitions()
                self._remove_partitions(current_zk_parts - new_partitions)
                self._add_partitions(new_partitions - current_zk_parts)
                if self._setup_internal_consumer(new_partitions):
                    log.info('Rebalancing Complete.')
                break
            except PartitionOwnedError as ex:
                if i == self._rebalance_max_retries - 1:
                    log.warning('Failed to acquire partition %s after %d retries.',
                                ex.partition, i)
                    raise
                log.info('Unable to acquire partition %s. Retrying', ex.partition)
                self._cluster.handler.sleep(i * (self._rebalance_backoff_ms / 1000))

    def _rebalance(self):
        """Start the rebalancing process for this consumer

        This method is called whenever a zookeeper watch is triggered.
        """
        # This Event is used to notify about rebalance operation to SimpleConsumer's consume().
        if not self._rebalancing_in_progress.is_set():
            self._rebalancing_in_progress.set()

        if self._consumer is not None:
            self.commit_offsets()
        # this is necessary because we can't stop() while the lock is held
        # (it's not an RLock)
        with self._rebalancing_lock:
            if not self._running:
                raise ConsumerStoppedException
            log.info('Rebalancing consumer "%s" for topic "%s".' % (
                self._consumer_id, self._topic.name))
            self._update_member_assignment()

        if self._rebalancing_in_progress.is_set():
            self._rebalancing_in_progress.clear()

    def _path_from_partition(self, p):
        """Given a partition, return its path in zookeeper.

        :type p: :class:`pykafka.partition.Partition`
        """
        return "%s/%s-%s" % (self._topic_path, p.leader.id, p.id)

    def _remove_partitions(self, partitions):
        """Remove partitions from the zookeeper registry for this consumer.

        :param partitions: The partitions to remove.
        :type partitions: Iterable of :class:`pykafka.partition.Partition`
        """
        for p in partitions:
            try:
                # TODO pass zk node version to make sure we still own this node
                self._zookeeper.delete(self._path_from_partition(p))
            except NoNodeException:
                pass

    def _add_partitions(self, partitions):
        """Add partitions to the zookeeper registry for this consumer.

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
            except NodeExistsError:
                raise PartitionOwnedError(p)

    def _get_held_partitions(self):
        """Build a set of partitions zookeeper says we own"""
        zk_partition_ids = set()
        all_partitions = self._zookeeper.get_children(self._topic_path)
        for partition_slug in all_partitions:
            try:
                owner_id, stat = self._zookeeper.get(
                    '{path}/{slug}'.format(
                        path=self._topic_path, slug=partition_slug))
                if owner_id == get_bytes(self._consumer_id):
                    zk_partition_ids.add(int(partition_slug.split('-')[1]))
            except NoNodeException:
                pass  # disappeared between ``get_children`` and ``get``
        return set(self._topic.partitions[_id] for _id in zk_partition_ids)

    @_catch_thread_exception
    def _brokers_changed(self, brokers):
        if not self._running:
            return False  # `False` tells ChildrenWatch to disable this watch
        if self._setting_watches:
            return
        log.debug("Rebalance triggered by broker change ({})".format(
            self._consumer_id))
        self._rebalance()

    @_catch_thread_exception
    def _consumers_changed(self, consumers):
        if not self._running:
            return False  # `False` tells ChildrenWatch to disable this watch
        if self._setting_watches:
            return
        log.debug("Rebalance triggered by consumer change ({})".format(
            self._consumer_id))
        self._rebalance()

    @_catch_thread_exception
    def _topics_changed(self, topics):
        if not self._running:
            return False  # `False` tells ChildrenWatch to disable this watch
        if self._setting_watches:
            return
        log.debug("Rebalance triggered by topic change ({})".format(
            self._consumer_id))
        self._rebalance()

    def reset_offsets(self, partition_offsets=None):
        """Reset offsets for the specified partitions

        For each value provided in `partition_offsets`: if the value is an integer,
        immediately reset the partition's internal offset counter to that value. If
        it's a `datetime.datetime` instance or a valid `OffsetType`, issue a
        `ListOffsetRequest` using that timestamp value to discover the latest offset
        in the latest log segment before that timestamp, then set the partition's
        internal counter to that value.

        :param partition_offsets: (`partition`, `timestamp_or_offset`) pairs to
            reset where `partition` is the partition for which to reset the offset
            and `timestamp_or_offset` is EITHER the timestamp before which to find
            a valid offset to set the partition's counter to OR the new offset the
            partition's counter should be set to
        :type partition_offsets: Sequence of tuples of the form
            (:class:`pykafka.partition.Partition`, int OR `datetime.datetime`)
        """
        self._raise_worker_exceptions()
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
            if not self._internal_consumer_running.is_set():
                self._cluster.handler.sleep()
                self._raise_worker_exceptions()
                self._internal_consumer_running.wait(self._consumer_timeout_ms / 1000)
            try:
                # acquire the lock to ensure that we don't start trying to consume from
                # a _consumer that might soon be replaced by an in-progress rebalance
                with self._rebalancing_lock:
                    message = self._consumer.consume(block=block, unblock_event=self._rebalancing_in_progress)

                # If Gevent is used, waiting to acquire _rebalancing lock introduces a race condition.
                # This sleep would ensure that the _rebalance method acquires the _rebalancing_lock
                # Issue: https://github.com/Parsely/pykafka/issues/671
                if self._rebalancing_in_progress.is_set():
                    self._cluster.handler.sleep()
            except (ConsumerStoppedException, AttributeError):
                if not self._running:
                    raise ConsumerStoppedException
            if message:
                self._last_message_time = time.time()
            if not block:
                return message
        return message

    def __iter__(self):
        """Yield an infinite stream of messages until the consumer times out"""
        while True:
            message = self.consume(block=True)
            if not message:
                return
            yield message

    def commit_offsets(self, partition_offsets=None):
        """Commit offsets for this consumer's partitions

        Uses the offset commit/fetch API

        :param partition_offsets: (`partition`, `offset`) pairs to
            commit where `partition` is the partition for which to commit the offset
            and `offset` is the offset to commit for the partition. Note that using
            this argument when `auto_commit_enable` is enabled can cause inconsistencies
            in committed offsets. For best results, use *either* this argument *or*
            `auto_commit_enable`.
        :type partition_offsets: Sequence of tuples of the form
            (:class:`pykafka.partition.Partition`, int)
        """
        self._raise_worker_exceptions()
        if not self._consumer:
            raise KafkaException("Cannot commit offsets - consumer not started")
        return self._consumer.commit_offsets(partition_offsets=partition_offsets)
