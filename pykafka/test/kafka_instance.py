"""
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
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time

from testinstances import utils
from testinstances.exceptions import ProcessNotStartingError
from testinstances.managed_instance import ManagedInstance

log = logging.getLogger(__name__)

_kafka_properties = """
# Configurable settings
broker.id={broker_id}
port={port}
zookeeper.connect={zk_connstr}
log.dirs={data_dir}

# non-configurable settings
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=1
num.recovery.threads.per.data.dir=1
log.segment.bytes=1073741824
log.cleaner.enable=false
zookeeper.connection.timeout.ms=6000
delete.topic.enable=true
"""

_zookeeper_properties = """
dataDir={zk_dir}
clientPort={zk_port}
maxClientCnxns=0
"""

class KafkaInstance(ManagedInstance):
    """A managed kafka instance for testing"""

    def __init__(self,
                 num_instances=1,
                 kafka_version='0.8.2.1',
                 bin_dir='/tmp/kafka-bin',
                 name='kafka',
                 use_gevent=False):
        """Start kafkainstace with given settings"""
        self._num_instances = num_instances
        self._kafka_version = kafka_version
        self._bin_dir = bin_dir
        self._processes = []
        self._zookeeper = None
        # TODO: Need a better name so multiple can run at once.
        #       other ManagedInstances use things like 'name-port'
        ManagedInstance.__init__(self, name, use_gevent=use_gevent)

    def _init_dirs(self):
        """Set up directories in the temp folder."""
        self._conf_dir = os.path.join(self._root_dir, 'conf')
        self._data_dir = os.path.join(self._root_dir, 'data')
        self._log_dir = os.path.join(self._root_dir, 'logs')
        self._zk_dir = os.path.join(self._root_dir, 'zk')
        os.makedirs(self._conf_dir)
        os.makedirs(self._data_dir)
        os.makedirs(self._log_dir)
        os.makedirs(self._zk_dir)

    def _download_kafka(self):
        """Make sure the Kafka code has been downloaded to the right dir."""
        binfile = os.path.join(self._bin_dir,
                               'bin/kafka-server-start.sh')
        if os.path.exists(binfile):
            return  # already there

        # Make the download dir
        try:
            os.makedirs(self._bin_dir)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        # Download and extract
        log.info('Downloading Kafka.')
        curr_dir = os.getcwd()
        os.chdir(self._bin_dir)
        url = 'http://mirror.reverse.net/pub/apache/kafka/{version}/kafka_2.10-{version}.tgz'.format(version=self._kafka_version)
        p1 = subprocess.Popen(['curl', '-vs', url], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['tar', 'xvz', '-C', self._bin_dir, '--strip-components', '1'], stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        output, err = p2.communicate()
        os.chdir(curr_dir)

        log.info('Downloaded Kafka to %s', self._bin_dir)

    def _is_port_open(self, port):
        """Check to see if a port is open"""
        try:
            s = socket.create_connection(('localhost', port))
            s.close()
            return False
        except IOError, err:
            return err.errno == errno.ECONNREFUSED

    def _port_generator(self, start):
        """Generate open ports, starting with `start`."""
        port = start
        while True:
            if self._is_port_open(port):
                yield port
            port += 1

    def _start_process(self):
        """Start the instance processes"""
        self._init_dirs()
        self._download_kafka()
        self._start_zookeeper()
        self._start_brokers()
        return

        # Connect to the shiny new instances
        self.conn = None
        fails = 0
        while self.conn is None:
            try:
                conn = pymongo.MongoClient(port=self.port, use_greenlets=self.use_gevent)
                if conn.alive():
                    self.conn = conn
            except:
                if fails == 10:
                    break
                fails += 1
                time.sleep(1)
        if self.conn is None or self._process.poll() is not None:
            raise ProcessNotStartingError("Unable to start mongod in 10 seconds.")

    def _start_log_watcher(self):
        """Overridden because we have multiple files to watch."""
        for logfile in os.listdir(self._log_dir):
            logfile = open(os.path.join(self._log_dir, logfile), 'r')
            if self.use_gevent:
                import gevent
                gevent.spawn(self._watcher_gevent, logfile)
            else:
                import threading
                watch_thread = threading.Thread(
                    target=self._watcher_threading, args=[logfile]
                )
                watch_thread.daemon = True
                watch_thread.start()

    def _start_brokers(self):
        self._broker_procs = []
        ports = self._port_generator(9092)
        used_ports = []
        for i in xrange(self._num_instances):
            port = ports.next()
            used_ports.append(port)
            log.info('Starting Kafka on port %i.', port)

            conf = os.path.join(self._conf_dir, 'kafka_{}.properties'.format(i))
            with open(conf, 'w') as f:
                f.write(_kafka_properties.format(
                    broker_id=i,
                    port=port,
                    zk_connstr=self._zookeeper,
                    data_dir=self._data_dir + '_{}'.format(i),
                ))

            binfile = os.path.join(self._bin_dir, 'bin/kafka-server-start.sh')
            logfile = os.path.join(self._log_dir, 'kafka_{}.log'.format(i))
            self._broker_procs.append(utils.Popen(
                args=[binfile, conf],
                stderr=utils.STDOUT,
                stdout=open(logfile, 'w'),
                use_gevent=self.use_gevent
            ))

        for port in used_ports:
            # Wait for it to start and open the port before moving on
            tries = 0
            while True:
                try:
                    s = socket.create_connection(('localhost', port))
                    s.close()
                    break
                except IOError, err:
                    tries += 1
                    if tries > 5:
                        raise ProcessNotStartingError(
                            'Unable to start Kafka on port %i', port
                        )
                    log.info('Waiting for broker to start....')
                    time.sleep(5)

    def _start_zookeeper(self):
        port = self._port_generator(2181).next()
        log.info('Starting zookeeper on port %i.', port)

        conf = os.path.join(self._conf_dir, 'zk.properties')
        with open(conf, 'w') as f:
            f.write(_zookeeper_properties.format(zk_dir=self._zk_dir,
                                                 zk_port=port))

        binfile = os.path.join(self._bin_dir, 'bin/zookeeper-server-start.sh')
        logfile = os.path.join(self._log_dir, 'zk.log')
        self._zk_proc = utils.Popen(
            args=[binfile, conf],
            stderr=utils.STDOUT,
            stdout=open(logfile, 'w'),
            use_gevent=self.use_gevent
        )
        self._zookeeper = 'localhost:{}'.format(port)

    def terminate(self):
        """Override because we have many processes."""
        # Kill brokers before zookeeper
        for broker in self._broker_procs:
            broker.terminate()
            broker.wait()
        self._zk_proc.terminate()
        self._zk_proc.wait()

        shutil.rmtree(self._root_dir)
        self._processes = []
        super(KafkaInstance, self).terminate()

    def flush(self):
        """Flush all data in the db"""
        raise NotImplementedError()
