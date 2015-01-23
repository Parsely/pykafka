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

import itertools
import json
import logging

from samsa import handlers, pysamsa
from samsa.common import Broker, Topic, Partition
from samsa.connection import BrokerConnection
from samsa.exceptions import ImproperlyConfiguredError
from samsa.protocol import MetadataRequest, MetadataResponse
from zlib import crc32

try:
    import rd_kafka
    RD_KAFKA_PRESENT = True
else:
    RD_KAFKA_PRESENT = False


logger = logging.getLogger(__name__)

class SamsaClient(object):
    """Main entry point for a Kafka cluster

    Notes:
        * Reconfiguring at any time is hard to coordinate. Updating
          the cluster should only happen on user actions?

    :ivar brokers: The :class:`samsa.common.Broker` map for this cluster.
    :ivar topics: The :class:`samsa.common.Topic` map for this cluster.
    """
    def __init__(self,
                 hosts='127.0.0.1:9092',
                 use_greenlets=False,
                 timeout=30,
                 use_rdkafka=True):
        """Create a connection to a Kafka cluster.

        :param hosts: Comma separated list of seed hosts to used to connect.
        :param use_greenlets: If True, gevent will be used instead of threading.
        :param timeout: Connection timeout, in seconds.
        :param use_librdkafka: Use rd_kafka, if installed.
        """
        self._seed_hosts = hosts
        self._timeout = timeout
        self.handler = None if use_greenlets else handlers.ThreadingHandler()
        self.use_rdkafka = RD_KAFKA_PRESENT and use_rdkafka
        if self.use_rdkafka:
            logger.info('Using rd_kafka extensions.')
            raise NotImplementedError('Not yet')
        else:
            self.cluster = pysamsa.Cluster(self._seed_hosts, self.handler)
        self.brokers = self.cluster.brokers

    def __getitem__(self, key):
        """Getter used to provide dict-like access to Topics."""
        return self.cluster.topics[key]

    def update_cluster(self):
        """Update known brokers and topics.

        Updates each Topic and Broker, adding new ones as found,
        with current metadata from the cluster.
        """
        # TODO: This is *so* not thread-safe, but updates should be rare.
        #       Consider making a single lock while this runs to basically
        #       stop the driver.
        self.cluster.update()
