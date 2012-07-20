import contextlib
import os
import subprocess
import random
import tempfile
import time

import mock
import unittest2
from nose.plugins.attrib import attr
from kazoo.testing import KazooTestHarness

from samsa.client import Client


class ClientTestCase(unittest2.TestCase):
    def test_produce(self):
        raise NotImplementedError

    def test_multiproduce(self):
        raise NotImplementedError

    def test_fetch(self):
        raise NotImplementedError

    def test_multifetch(self):
        raise NotImplementedError

    def test_offsets(self):
        raise NotImplementedError


@attr('integration')
class ClientIntegrationTestCase(unittest2.TestCase, KazooTestHarness):
    def setUp(self):
        self.setup_zookeeper()
        self.setup_kafka()

    def tearDown(self):
        self.teardown_kafka()
        self.teardown_zookeeper()

    def setup_kafka(self):
        directory = tempfile.mkdtemp()
        server = random.choice(list(self.cluster))
        configuration = {
            'brokerid': 0,
            'port': 9092,
            'log.dir': directory,
            'num.partitions': 1,
            'enable.zookeeper': 'true',
            'zk.connect': server.address,
        }

        values = ['='.join(map(str, item)) for item in configuration.items()]
        rendered_configuration = '\n'.join(values)

        configuration_file = tempfile.NamedTemporaryFile(delete=False)
        with contextlib.closing(configuration_file):
            configuration_file.write(rendered_configuration)
            configuration_file.flush()

        executable = os.path.join(os.path.dirname(__file__), '..', '..', 'kafka-run-class.sh')
        self.kafka = subprocess.Popen([executable, 'kafka.Kafka', configuration_file.name])
        time.sleep(1)

    def teardown_kafka(self):
        self.kafka.terminate()

        timeout = 5
        for _ in xrange(0, timeout):
            self.kafka.poll()
            if self.kafka.returncode is None:
                time.sleep(1)
            else:
                break
        else:
            self.kafka.kill()

    def test_produce(self):
        raise NotImplementedError

    def test_multiproduce(self):
        raise NotImplementedError

    def test_fetch(self):
        raise NotImplementedError

    def test_multifetch(self):
        raise NotImplementedError

    def test_offsets(self):
        raise NotImplementedError
