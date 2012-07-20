import contextlib
import os
import subprocess
import random
import tempfile
import time

import unittest2
from kazoo.testing import KazooTestHarness


class IntegrationTestCase(unittest2.TestCase, KazooTestHarness):
    def setUp(self):
        self.setup_zookeeper()
        self.setup_kafka()

    def tearDown(self):
        self.teardown_kafka()
        self.teardown_zookeeper()

    def setup_kafka(self):
        self._kafka_directory = tempfile.mkdtemp()
        server = random.choice(list(self.cluster))
        configuration = {
            'brokerid': 0,
            'port': 9092,
            'log.dir': self._kafka_directory,
            'num.partitions': 1,
            'enable.zookeeper': 'true',
            'zk.connect': server.address,
        }

        self._kafka_configuration_file = tempfile.NamedTemporaryFile(delete=False)
        with contextlib.closing(self._kafka_configuration_file):
            self._kafka_configuration_file.writelines('='.join(map(str, item))
                for item in configuration.items())
            self._kafka_configuration_file.flush()

        executable = os.path.join(os.path.dirname(__file__), 'kafka-run-class.sh')

        null = open('/dev/null')
        args = [executable, 'kafka.Kafka', self._kafka_configuration_file.name]
        self._kafka_broker = subprocess.Popen(args, stderr=null, stdout=null)
        time.sleep(1)

    def teardown_kafka(self):
        self._kafka_broker.terminate()

        timeout = 5
        for _ in xrange(0, timeout):
            self._kafka_broker.poll()
            if self._kafka_broker.returncode is None:
                time.sleep(1)
            else:
                break
        else:
            self._kafka_broker.kill()

        # TODO: Clean up temporary directory and configuration files.

IntegrationTestCase.integration = True
