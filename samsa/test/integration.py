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
    return reduce(lambda x, y: dict(x, **y), dicts, {})


class TimeoutError(Exception):
    pass


def polling_timeout(predicate, duration, interval=0.1, error='timeout exceeded'):
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

    def tearDown(self):
        self.teardown_kafka_broker()
        self.teardown_zookeeper()

    def __run_class(self, cls, *args, **kwargs):
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
            while process.returncode is None:
                line = process.stderr.readline().strip()
                if not line:
                    continue

                try:
                    name, level, message = (bit.strip() for bit in line.split(':', 2))
                    logging.getLogger('%s:%s' % (namespace, name)).log(getattr(logging, level.upper()), message)
                except ValueError:
                    logging.getLogger('%s:unfiltered' % namespace).warning(line)

        logthread = threading.Thread(target=convert_log_output, args=(process, cls))
        logthread.daemon = True  # shouldn't be necessary, but just in case
        logthread.start()

        return process

    def __write_property_file(self, properties):
        file = tempfile.NamedTemporaryFile(delete=False)
        for item in properties.iteritems():
            file.write('%s=%s\n' % item)
        file.close()
        return file

    def setup_kafka_broker(self):
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
        self.kafka_broker.terminate()

        def broker_exited():
            self.kafka_broker.poll()
            return self.kafka_broker.returncode is not None

        logger.debug('Shutting down Kafka broker...')
        try:
            polling_timeout(broker_exited, 3)
        except TimeoutError:
            logger.info('Broker did not exit cleanly within timeout, sending SIGKILL...')
            self.kafka_broker.kill()
        finally:
            # TODO: Clean up temporary directory and configuration files.
            pass

    def consumer(self, topic, group='test-consumer-group'):
        # TODO: Clean up after self.
        configuration_file = self.__write_property_file({
            'zk.connect': self.hosts,
            'groupid': group,
        })
        return self.__run_class('kafka.tools.ConsumerShell',
            '--topic', topic, '--props', configuration_file.name,
            stdout=subprocess.PIPE)

    def producer(self, topic, **kwargs):
        # TODO: Clean up after self.
        configuration_file = self.__write_property_file({
            'zk.connect': self.hosts,
            'producer.type': 'sync',
            'compression.codec': 0,
            'serializer.class': 'kafka.serializer.StringEncoder',
        })
        return self.__run_class('kafka.tools.ProducerShell',
            '--topic', topic, '--props', configuration_file.name,
            **kwargs)
