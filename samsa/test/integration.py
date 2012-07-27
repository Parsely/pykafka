import logging
import os
import subprocess
import random
import tempfile
import threading
import time

import unittest2
from kazoo.testing import KazooTestHarness
from nose.plugins.attrib import attr


logger = logging.getLogger(__name__)


def merge(*dicts):
    """
    Merges a sequence of dictionaries, with values in later dictionaries
    overwriting values in the earlier dictionaries.
    """
    return reduce(lambda x, y: dict(x, **y), dicts, {})


class TimeoutError(Exception):
    pass


def polling_timeout(predicate, duration, interval=0.1, error='timeout exceeded'):
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


@attr('integration')
class KafkaIntegrationTestCase(unittest2.TestCase, KazooTestHarness):
    KAFKA_BROKER_CONFIGURATION = {
        'brokerid': 0,
        'port': 9092,
        'num.partitions': 1,
        'enable.zookeeper': 'true',
        'log.flush.interval': 1,  # commit messages as soon as possible
    }

    KAFKA_OPTIONS = {
        'heap_max': '512M',
    }

    KAFKA_LOGGING_CONFIGURATION = {
        'log4j.rootLogger': 'INFO, stderr',
        'log4j.appender.stderr': 'org.apache.log4j.ConsoleAppender',
        'log4j.appender.stderr.target': 'System.err',
        'log4j.appender.stderr.layout': 'org.apache.log4j.PatternLayout',
        'log4j.appender.stderr.layout.ConversionPattern': '%c: %p: %m%n',
        'log4j.logger.kafka': 'INFO',
    }

    def setUp(self):
        self.setup_zookeeper()
        self.setup_kafka_broker()

        self.subprocesses = []

    def tearDown(self):
        self.teardown_kafka_broker()
        self.teardown_zookeeper()

        for subprocess in self.subprocesses:
            if subprocess.returncode is None:
                self.clean_shutdown(subprocess)

    def clean_shutdown(self, process, timeout=3):
        logger.debug('Sending SIGTERM to %s', process)

        def process_exited():
            process.poll()
            return process.returncode is not None

        process.terminate()
        try:
            polling_timeout(process_exited, timeout)
            logger.debug('%s exited cleanly', process)
        except TimeoutError:
            logger.info('%s did not exit within timeout, sending SIGKILL...', process)
            process.kill()

    def __run_class(self, cls, *args, **kwargs):
        """
        Runs the provided Kafka class in a subprocess.

        Logging from the subprocess via stderr is converted into Python logging
        statements in a separate thread of execution.
        """
        executable = os.path.join(os.path.dirname(__file__), 'kafka-run-class.sh')
        args = (executable, cls) + args

        kwargs['env'] = kwargs.get('env', os.environ).copy()
        kwargs['env'].update({
            'KAFKA_OPTS': '-Xmx%(heap_max)s -server -Dlog4j.configuration=file:%(logging_config)s' % \
                merge(self.KAFKA_OPTIONS, {
                    'logging_config': self.__kafka_logging_configuration_file.name,
                })
        })

        process = subprocess.Popen(args, stderr=subprocess.PIPE, **kwargs)

        def convert_log_output(process, namespace):
            """
            Scrapes log4j output, forwarding the log output to the corresponding
            Python logging endpoint.
            """
            while process.returncode is None:
                line = process.stderr.readline().strip()
                if not line:
                    continue

                try:
                    name, level, message = (bit.strip() for bit in line.split(':', 2))
                    logging.getLogger('%s.%s' % (namespace, name)).log(getattr(logging, level.upper()), message)
                except ValueError:
                    logging.getLogger('%s.raw' % namespace).warning(line)

        logthread = threading.Thread(target=convert_log_output,
            args=(process, 'java.%s' % cls))
        logthread.daemon = True  # shouldn't be necessary, but just in case
        logthread.start()

        return process

    def __write_property_file(self, properties):
        """
        Writes a dictionary if configuration properties to a file readable
        by Kafka.
        """
        file = tempfile.NamedTemporaryFile(delete=False)
        for item in properties.iteritems():
            file.write('%s=%s\n' % item)
        file.close()
        return file

    def setup_kafka_broker(self):
        """
        Starts the Kafka broker.
        """
        self.__kafka_directory = tempfile.mkdtemp()

        # Logging Configuratin
        self.__kafka_logging_configuration_file = \
            self.__write_property_file(self.KAFKA_LOGGING_CONFIGURATION)

        # Broker Configuration
        broker_configuration = merge(self.KAFKA_BROKER_CONFIGURATION, {
            'log.dir': self.__kafka_directory,
            'zk.connect': self.hosts,
        })
        self.__kafka_broker_configuration_file = \
            self.__write_property_file(broker_configuration)

        ready = threading.Event()
        path = '/brokers/ids/%(brokerid)s' % broker_configuration
        if self.client.exists(path, watch=lambda *a, **k: ready.set()) is not None:
            raise AssertionError('Kafka broker is already running!')

        logger.info('Starting Kafka broker...')
        self.kafka_broker = self.__run_class('kafka.Kafka', self.__kafka_broker_configuration_file.name)

        timeout = 3
        for _ in xrange(0, timeout):
            if ready.is_set():
                break
            ready.wait(timeout=1)
        else:
            raise TimeoutError('Kafka broker did not start within %s seconds' % timeout)

    def teardown_kafka_broker(self):
        """
        Stops the Kafka broker.
        """
        logger.debug('Shutting down Kafka broker...')
        self.clean_shutdown(self.kafka_broker)

    def consumer(self, topic, group='test-consumer-group', **kwargs):
        """
        Returns a subprocess for a consumer shell for the given topic and
        consumer group.
        """
        # TODO: Clean up after self.
        configuration_file = self.__write_property_file({
            'zk.connect': self.hosts,
            'groupid': group,
        })
        process = self.__run_class('kafka.tools.ConsumerShell',
            '--topic', topic, '--props', configuration_file.name,
            stdout=subprocess.PIPE, **kwargs)
        self.subprocesses.append(process)
        return process

    def producer(self, topic, **kwargs):
        """
        Returns a subprocess for a producer shell for the given topic.
        """
        # TODO: Clean up after self.
        configuration_file = self.__write_property_file({
            'zk.connect': self.hosts,
            'producer.type': 'sync',
            'compression.codec': 0,
            'serializer.class': 'kafka.serializer.StringEncoder',
        })
        process = self.__run_class('kafka.tools.ProducerShell',
            '--topic', topic, '--props', configuration_file.name,
            **kwargs)
        self.subprocesses.append(process)
        return process
