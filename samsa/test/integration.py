__license__ = """
Copyright 2012 DISQUS
Copyright 2013 Parse.ly, Inc.

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

import errno
import itertools
import kazoo.handlers.threading
import logging
import os
import socket
import subprocess
import tempfile
import threading
import time
import uuid

from kazoo.testing import KazooTestHarness
from nose.plugins.attrib import attr

from samsa import handlers
from samsa.client import Client
from samsa.cluster import Cluster
from samsa.test.case import TestCase
from samsa.utils.functional import methodmap


logger = logging.getLogger(__name__)


def is_port_available(port):
    """
    Checks to see if the local port is available for use.
    """
    try:
        s = socket.create_connection(('localhost', port))
        s.close()
        return False
    except IOError, err:
        return err.errno == errno.ECONNREFUSED


def merge(*dicts):
    """
    Merges a sequence of dictionaries, with values in later dictionaries
    overwriting values in the earlier dictionaries.
    """
    return reduce(lambda x, y: dict(x, **y), dicts, {})


class TimeoutError(Exception):
    pass


def polling_timeout(predicate, duration, interval=0.1,
        error='timeout exceeded'):
    """
    Provides a blocking timeout until either the predicate function returns a
    non-False value, or the timeout is exceeded in which case a `TimeoutError`
    is raised.
    """
    for _ in xrange(0, int(duration / interval)):
        if predicate():
            return
        time.sleep(interval)
    else:
        raise TimeoutError(error)


def write_property_file(properties):
    """
    Writes a dictionary if configuration properties to a file readable
    by Kafka.
    """
    file = tempfile.NamedTemporaryFile(delete=False)
    for item in properties.iteritems():
        file.write('%s=%s\n' % item)
    file.close()
    return file


class ExternalClassRunner(object):
    args = []
    kwargs = {}
    script = os.path.join(os.path.dirname(__file__), 'kafka-run-class.sh')

    stop_timeout = 3

    KAFKA_OPTIONS = {
        'heap_max': '512M',
    }

    LOGGING_CONFIGURATION = {
        'log4j.rootLogger': 'INFO, stderr',
        'log4j.appender.stderr': 'org.apache.log4j.ConsoleAppender',
        'log4j.appender.stderr.target': 'System.err',
        'log4j.appender.stderr.layout': 'org.apache.log4j.PatternLayout',
        'log4j.appender.stderr.layout.ConversionPattern': '%c: %p: %m%n',
        'log4j.logger.kafka': 'INFO',
    }

    def __init__(self):
        self.logging_configuration_file = \
            write_property_file(self.LOGGING_CONFIGURATION)

    def start(self):
        args = ['/bin/sh', self.script, self.cls] + self.args

        kwargs = self.kwargs.copy()
        kwargs['env'] = kwargs.get('env', os.environ).copy()
        kwargs['env'].update({
            'KAFKA_OPTS': '-Xmx%(heap_max)s -server '
                '-Dlog4j.configuration=file:%(logging_config)s' %
                merge(self.KAFKA_OPTIONS, {
                    'logging_config': self.logging_configuration_file.name,
                })
        })

        self.process = subprocess.Popen(args, stderr=subprocess.PIPE, **kwargs)

        def convert_log_output(namespace):
            """
            Scrapes log4j output, forwarding the log output to the
            corresponding Python logging endpoint.
            """
            while True:
                line = self.process.stderr.readline().strip()
                if not line:
                    continue

                try:
                    name, level, message = \
                        methodmap('strip', line.split(':', 2))
                    logger = logging.getLogger('%s.%s' % (namespace, name))
                    logger.log(getattr(logging, level.upper()), message)
                except Exception:
                    logger = logging.getLogger('%s.raw' % namespace)
                    logger.warning(line)

        self.log_thread = threading.Thread(target=convert_log_output,
            args=('java.%s' % self.cls,))
        self.log_thread.daemon = True  # shouldn't be necessary, just in case
        self.log_thread.start()

    def is_running(self):
        if not hasattr(self, 'process'):
            return False

        self.process.poll()
        return self.process.returncode is None

    def stop(self, timeout=None):
        if not self.is_running():
            return

        if timeout is None:
            timeout = self.stop_timeout

        logger.debug('Sending SIGTERM to %s...', self.process)

        self.process.terminate()
        # Can block, but we're reading from the pipe so it should be fine.
        self.process.wait()


class ManagedBroker(ExternalClassRunner):
    cls = 'kafka.Kafka'

    start_timeout = int(os.environ.get('KAFKA_START_TIMEOUT', 3))

    CONFIGURATION = {
        'enable.zookeeper': 'true',
        'log.flush.interval': 1,  # commit messages as soon as possible
    }

    def __init__(self, zookeeper, hosts, brokerid=0, partitions=1, port=9092):
        super(ManagedBroker, self).__init__()
        self.directory = tempfile.mkdtemp()

        self.zookeeper = zookeeper
        self.brokerid = brokerid
        self.partitions = partitions
        self.port = port

        self.configuration = merge(self.CONFIGURATION, {
            'log.dir': self.directory,
            'zk.connect': hosts,
            'brokerid': self.brokerid,
            'num.partitions': self.partitions,
            'hostname': '127.0.0.1', # travis-ci will try to make this ipv6
            'port': self.port,
        })

        self.configuration_file = write_property_file(self.configuration)
        self.args = [self.configuration_file.name]
        self._client = None

    def start(self, timeout=None):
        if timeout is None:
            timeout = self.start_timeout

        ready = threading.Event()
        path = '/brokers/ids/%s' % self.brokerid
        callback = lambda *a, **k: ready.set()
        if self.zookeeper.exists(path, watch=callback) is not None:
            raise AssertionError('Kafka broker with broker ID %s is already '
                'running!' % self.brokerid)

        super(ManagedBroker, self).start()

        for _ in xrange(0, timeout):
            if ready.is_set():
                break
            ready.wait(1)
        else:
            raise TimeoutError('Kafka broker did not start within %s seconds'
                % timeout)

    def stop(self, *args, **kwargs):
        logger.debug('Shutting down Kafka broker...')
        super(ManagedBroker, self).stop(*args, **kwargs)
        # TODO: Remove configuration, log dir

    @property
    def client(self):
        assert self.is_running()
        if not self._client:
            self._client = Client(
                'localhost', handlers.ThreadingHandler(),
                port=self.port
            )
        return self._client


class ManagedProducer(ExternalClassRunner):
    cls = 'kafka.tools.ProducerShell'
    kwargs = {
        'stdout': open('/dev/null'),
        'stdin': subprocess.PIPE,
    }

    CONFIGURATION = {
        'producer.type': 'sync',
        'compression.codec': 0,
        'serializer.class': 'kafka.serializer.StringEncoder',
    }

    def __init__(self, hosts, topic):
        super(ManagedProducer, self).__init__()
        self.hosts = hosts
        self.topic = topic

        self.configuration_file = write_property_file(merge(
            self.CONFIGURATION, {
                'zk.connect': self.hosts,
            }))

        self.args = ['--topic', self.topic,
            '--props', self.configuration_file.name]

    def publish(self, messages):
        stream = self.process.stdin
        for message in messages:
            stream.write('%s\n' % message)
        stream.flush()


class ManagedConsumer(ExternalClassRunner):
    cls = 'kafka.tools.ConsumerShell'
    kwargs = {
        'stdout': subprocess.PIPE,
    }

    def __init__(self, hosts, topic, group='test-consumer-group'):
        super(ManagedConsumer, self).__init__()
        self.hosts = hosts
        self.topic = topic
        self.group = group

        self.configuration_file = write_property_file({
            'zk.connect': self.hosts,
            'groupid': self.group,
        })

        self.args = ['--topic', self.topic,
            '--props', self.configuration_file.name]


@attr('integration')
class KafkaIntegrationTestCase(TestCase, KazooTestHarness):
    """
    A test case that allows the bootstrapping of a number of Kafka brokers.

    Brokers are restarted with new log directories for each test method
    invocation, ensuring that individual tests do not affect the results of
    others.

    All managed subprocesses (brokers, consumers, producers, etc.) are
    automatically stopped when the test case is torn down.
    """

    def setUp(self):
        """Set up kafka and zookeeper."""
        try:
            self.setup_zookeeper()
        except kazoo.handlers.threading.TimeoutError:
            logging.warning('Zookeeper failed to start. Trying again.')
            if self.cluster[0].running:
                self.cluster.stop()
            self.setup_zookeeper() # try again if travis-ci is being slow

        # Keep track of processes started
        self._consumers = []
        self._producers = []

        self.kafka_broker = self.start_broker(self.client, self.hosts)
        self.kafka_cluster = Cluster(self.client)

    def tearDown(self):
        """Reset zookeeper and Kafka if needed"""
        self.client.stop()
        for process in itertools.chain(self._consumers, self._producers):
            process.stop()
        if self.kafka_broker and self.kafka_broker.is_running():
            self.kafka_broker.stop()
        self.teardown_zookeeper()

    @classmethod
    def start_broker(cls, zk_client, zk_hosts, brokerid=0):
        ports = itertools.ifilter(
            is_port_available, itertools.count(9092)
        )
        broker = ManagedBroker(
            zk_client,
            zk_hosts,
            brokerid=brokerid,
            port=next(ports),
        )
        broker.start()
        return broker

    def consumer(self, *args, **kwargs):
        consumer = ManagedConsumer(self.hosts, *args, **kwargs)
        consumer.start()
        self._consumers.append(consumer)
        return consumer

    def producer(self, *args, **kwargs):
        producer = ManagedProducer(self.hosts, *args, **kwargs)
        producer.start()
        self._producers.append(producer)
        return producer

    def get_topic(self):
        """Get a random topic to use for testing."""
        topic = str(uuid.uuid4())
        return self.kafka_cluster.topics[topic]


class FasterKafkaIntegrationTestCase(KafkaIntegrationTestCase):
    """A faster test case that doesn't restart Kafka between tests.

    To use this class, use `self.get_topic()` instead of hard-coding
    the topic name. This lets the same broker be used for all tests
    without having to worry about collisions between tests.

    This exists because kafka can't delete topics, requiring a full
    restart between test cases. This is very slow and not strictly
    necessary for most tests.

    If your test *needs* the broker to restart between cases, or you're
    testing the broker going down, etc, then use `KafkaIntegrationTestCase`.
    However, for most tests, this is the class you want.
    """

    @classmethod
    def setUpClass(cls):
        cls.zk_harness = KazooTestHarness()
        cls.zk_harness.setup_zookeeper()
        cls.client = cls.zk_harness.client
        cls.hosts = cls.zk_harness.hosts
        cls.kafka_broker = cls.start_broker(cls.client, cls.hosts)
        cls.kafka_cluster = Cluster(cls.client)

    @classmethod
    def tearDownClass(cls):
        cls.client.stop()
        if cls.kafka_broker and cls.kafka_broker.is_running():
            cls.kafka_broker.stop()
        cls.zk_harness.teardown_zookeeper()

    def setUp(self):
        # Keep track of processes started
        self._consumers = []
        self._producers = []

    def tearDown(self):
        """Only need to stop consumer/producer processes"""
        for process in itertools.chain(self._consumers, self._producers):
            process.stop()
