__license__ = """
Copyright 2012 DISQUS

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
import logging
import os
import socket
import subprocess
import tempfile
import threading
import time

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
    executable = os.path.join(os.path.dirname(__file__), 'kafka-run-class.sh')

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
        args = [self.executable, self.cls] + self.args

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
        try:
            polling_timeout(lambda: not self.is_running(), timeout)
            logger.debug('%s exited cleanly', self.process)
        except TimeoutError:
            logger.info('%s did not exit within %s timeout, sending '
                'SIGKILL...', timeout, self.process)
            self.process.kill()


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
class KafkaClusterIntegrationTestCase(TestCase, KazooTestHarness):
    """
    A test case that allows the bootstrapping of a number of Kafka brokers.

    Brokers are restarted with new log directories for each test method
    invocation, ensuring that individual tests do not affect the results of
    others.

    All managed subprocesses (brokers, consumers, producers, etc.) are
    automatically stopped when the test case is torn down.
    """
    def setUp(self):
        self.setup_zookeeper()
        self._subprocesses = []

        self._id_generator = itertools.count(0)
        self._port_generator = itertools.ifilter(is_port_available,
            itertools.count(9092))

        self.kafka_cluster = Cluster(self.client)

    def tearDown(self):
        for process in self._subprocesses:
            if process.is_running():
                process.stop()

        self.teardown_zookeeper()

    def setup_kafka_broker(self, *args, **kwargs):
        """
        Starts a Kafka broker.

        The broker is started using a sequence generated broker ID and port to
        avoid conflicts.

        :rtype: :class:`~samsa.test.integration.ManagedBroker`
        """
        broker = ManagedBroker(self.client, self.hosts,
            brokerid=next(self._id_generator),
            port=next(self._port_generator), *args, **kwargs)
        broker.start()
        self._subprocesses.append(broker)
        return broker

    def consumer(self, *args, **kwargs):
        consumer = ManagedConsumer(self.hosts, *args, **kwargs)
        consumer.start()
        self._subprocesses.append(consumer)
        return consumer

    def producer(self, *args, **kwargs):
        producer = ManagedProducer(self.hosts, *args, **kwargs)
        producer.start()
        self._subprocesses.append(producer)
        return producer


@attr('integration')
class KafkaIntegrationTestCase(KafkaClusterIntegrationTestCase):
    """
    A test case that automatically starts a single Kafka broker available as
    :attr:`kafka_broker` on each test method invocation.
    """
    def setUp(self):
        super(KafkaIntegrationTestCase, self).setUp()
        self.kafka_broker = self.setup_kafka_broker()
