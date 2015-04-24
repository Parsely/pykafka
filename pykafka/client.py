__license__ = """
Copyright 2012 DISQUS
Copyright 2013,2014 Parse.ly, Inc.

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

import logging

import handlers
from cluster import Cluster

try:
    import rd_kafka
except ImportError:
    rd_kafka = None


logger = logging.getLogger(__name__)


class KafkaClient(object):
    """Main entry point for a Kafka cluster

    :ivar brokers: The :class:`kafka.common.Broker` map for this cluster.
    :ivar topics: The :class:`kafka.common.Topic` map for this cluster.
    """
    def __init__(self,
                 hosts='127.0.0.1:9092',
                 use_greenlets=False,
                 socket_timeout_ms=30 * 1000,
                 offsets_channel_socket_timeout_ms=10 * 1000,
                 ignore_rdkafka=False,
                 socket_receive_buffer_bytes=64 * 1024,
                 exclude_internal_topics=True):
        """Create a connection to a Kafka cluster.

        :param hosts: Comma separated list of seed hosts to used to connect.
        :param use_greenlets: If True, use gevent instead of threading.
        :param socket_timeout_ms: the socket timeout for network requests
        :type socket_timeout_ms: int
        :param offsets_channel_socket_timeout_ms: Socket timeout when reading
            responses for offset fetch/commit requests.
        :type offsets_channel_socket_timeout_ms: int
        :param ignore_rdkafka: Don't use rdkafka, even if installed.
        :param socket_receive_buffer_bytes: the size of the socket receive
            buffer for network requests
        :type socket_receive_buffer_bytes: int
        :param exclude_internal_topics: Whether messages from internal topics
            (such as offsets) should be exposed to the consumer.
        :type exclude_internal_topics: bool
        """
        self._seed_hosts = hosts
        self._socket_timeout_ms = socket_timeout_ms
        self._offsets_channel_socket_timeout_ms = offsets_channel_socket_timeout_ms
        self._handler = None if use_greenlets else handlers.ThreadingHandler()
        self._use_rdkafka = rd_kafka and not ignore_rdkafka
        if self._use_rdkafka:
            logger.info('Using rd_kafka extensions.')
            raise NotImplementedError('Not yet')
        else:
            self.cluster = Cluster(
                self._seed_hosts,
                self._handler,
                socket_timeout_ms=self._socket_timeout_ms,
                offsets_channel_socket_timeout_ms=self._offsets_channel_socket_timeout_ms,
                socket_receive_buffer_bytes=socket_receive_buffer_bytes,
                exclude_internal_topics=exclude_internal_topics
            )
        self.brokers = self.cluster.brokers
        self.topics = self.cluster.topics

    def update_cluster(self):
        """Update known brokers and topics.

        Updates each Topic and Broker, adding new ones as found,
        with current metadata from the cluster.
        """
        # TODO: This is *so* not thread-safe, but updates should be rare.
        #       Consider making a single lock while this runs to basically
        #       stop the driver.
        self.cluster.update()
