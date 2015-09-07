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

import argparse
import errno
import logging
import os
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import time

from testinstances import utils
from testinstances.exceptions import ProcessNotStartingError
from testinstances.managed_instance import ManagedInstance
from pykafka.utils.compat import range, get_bytes, get_string


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

class KafkaConnection(object):
    """Connection to a Kafka cluster.

    Provides handy access to the shell scripts Kafka is bundled with.
    """

    def __init__(self, bin_dir, brokers, zookeeper):
        """Create a connection to the cluster.

        :param bin_dir:   Location of downloaded kafka bin
        :param brokers:   Comma-separated list of brokers
        :param zookeeper: Connection straing for ZK
        """
        self._bin_dir = bin_dir
        self.brokers = brokers
        self.zookeeper = zookeeper

    def _run_topics_sh(self, args):
        """Run kafka-topics.sh with the provided list of arguments."""
        binfile = os.path.join(self._bin_dir, 'bin/kafka-topics.sh')
        cmd = [binfile, '--zookeeper', self.zookeeper] + args
        cmd = [get_string(c) for c in cmd]  # execv needs only strings
        log.debug('running: %s', ' '.join(cmd))
        return subprocess.check_output(cmd)

    def create_topic(self, topic_name, num_partitions, replication_factor):
        """Use kafka-topics.sh to create a topic."""
        log.info('Creating topic %s', topic_name)
        self._run_topics_sh(['--create',
                             '--topic', topic_name,
                             '--partitions', num_partitions,
                             '--replication-factor', replication_factor])
        time.sleep(2)

    def delete_topic(self, topic_name):
        self._run_topics_sh(['--delete',
                             '--topic', topic_name])

    def flush(self):
        """Delete all topics."""
        for topic in self.list_topics():
            self.delete_topic(topic)

    def list_topics(self):
        """Use kafka-topics.sh to get topic information."""
        res = self._run_topics_sh(['--list'])
        return res.strip().split(b'\n')

    def produce_messages(self, topic_name, messages, batch_size=200):
        """Produce some messages to a topic."""
        binfile = os.path.join(self._bin_dir, 'bin/kafka-console-producer.sh')
        cmd = [binfile,
               '--broker-list', self.brokers,
               '--topic', topic_name,
               '--batch-size', batch_size]
        cmd = [get_string(c) for c in cmd]  # execv needs only strings
        log.debug('running: %s', ' '.join(cmd))
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        proc.communicate(input=get_bytes('\n'.join(messages)))
        if proc.poll() is None:
            proc.kill()


class KafkaInstance(ManagedInstance):
    """A managed kafka instance for testing"""

    def __init__(self,
                 num_instances=1,
                 kafka_version='0.8.2.1',
                 scala_version='2.10',
                 bin_dir='/tmp/kafka-bin',
                 name='kafka',
                 use_gevent=False):
        """Start kafkainstace with given settings"""
        self._num_instances = num_instances
        self._kafka_version = kafka_version
        self._scala_version = scala_version
        self._bin_dir = bin_dir
        self._processes = []
        self.zookeeper = None
        self.brokers = None
        # TODO: Need a better name so multiple can run at once.
        #       other ManagedInstances use things like 'name-port'
        ManagedInstance.__init__(self, name, use_gevent=use_gevent)
        self.connection = KafkaConnection(bin_dir, self.brokers, self.zookeeper)

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
        # Needs to exist so that testinstances doesn't break
        open(self.logfile, 'w').close()

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
        url_fmt = 'http://mirror.reverse.net/pub/apache/kafka/{kafka_version}/kafka_{scala_version}-{kafka_version}.tgz'
        url = url_fmt.format(
            scala_version=self._scala_version,
            kafka_version=self._kafka_version
        )
        p1 = subprocess.Popen(['curl', '-vs', url], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['tar', 'xvz', '-C', self._bin_dir,
                               '--strip-components', '1'],
                              stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        output, err = p2.communicate()
        os.chdir(curr_dir)

        log.info('Downloaded Kafka to %s', self._bin_dir)

    def _is_port_free(self, port):
        """Check to see if a port is open"""
        try:
            s = socket.create_connection(('localhost', port))
            s.close()
            return False
        except IOError as err:
            return err.errno == errno.ECONNREFUSED

    def _port_generator(self, start):
        """Generate open ports, starting with `start`."""
        port = start
        while True:
            if self._is_port_free(port):
                yield port
            port += 1

    def _start_process(self):
        """Start the instance processes"""
        self._init_dirs()
        self._download_kafka()

        # Start all relevant processes and save which ports they use
        zk_port = self._start_zookeeper()
        self.zookeeper = 'localhost:{port}'.format(port=zk_port)

        broker_ports = self._start_brokers()
        self.brokers = ','.join('localhost:{port}'.format(port=port)
                               for port in broker_ports)

        # Process is started when the port isn't free anymore
        all_ports = [zk_port] + broker_ports
        for i in range(10):
            if all(not self._is_port_free(port) for port in all_ports):
                log.info('Kafka cluster started.')
                return  # hooray! success
            log.info('Waiting for cluster to start....')
            time.sleep(6)  # Waits 60s total

        # If it got this far, it's an error
        raise ProcessNotStartingError('Unable to start Kafka cluster.')

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
        """Start all brokers and return used ports."""
        self._broker_procs = []
        ports = self._port_generator(9092)
        used_ports = []
        for i in range(self._num_instances):
            port = next(ports)
            used_ports.append(port)
            log.info('Starting Kafka on port %i.', port)

            conf = os.path.join(self._conf_dir,
                                'kafka_{instance}.properties'.format(instance=i))
            with open(conf, 'w') as f:
                f.write(_kafka_properties.format(
                    broker_id=i,
                    port=port,
                    zk_connstr=self.zookeeper,
                    data_dir=self._data_dir + '_{instance}'.format(instance=i),
                ))

            binfile = os.path.join(self._bin_dir, 'bin/kafka-server-start.sh')
            logfile = os.path.join(self._log_dir, 'kafka_{instance}.log'.format(instance=i))
            self._broker_procs.append(utils.Popen(
                args=[binfile, conf],
                stderr=utils.STDOUT,
                stdout=open(logfile, 'w'),
                use_gevent=self.use_gevent
            ))
        return used_ports

    def _start_zookeeper(self):
        port = next(self._port_generator(2181))
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
        return port

    def terminate(self):
        """Override because we have many processes."""
        # Kill brokers before zookeeper
        log.info('Stopping Kafka brokers.')
        for broker in self._broker_procs:
            broker.kill()
        log.info('Stopping Zookeeper.')
        self._zk_proc.kill()

        shutil.rmtree(self._root_dir)
        self._processes = []
        super(KafkaInstance, self).terminate()
        log.info('Cluster terminated and data cleaned up.')

    def flush(self):
        """Flush all data in the cluster"""
        raise NotImplementedError()

    def create_topic(self, topic_name, num_partitions, replication_factor):
        """Use kafka-topics.sh to create a topic."""
        return self.connection.create_topic(topic_name, num_partitions,
                                            replication_factor)

    def delete_topic(self, topic_name):
        return self.connection.delete_topic(topic_name)

    def list_topics(self):
        """Use kafka-topics.sh to get topic information."""
        return self.connection.list_topics()

    def produce_messages(self, topic_name, messages):
        """Produce some messages to a topic."""
        return self.connection.produce_messages(topic_name, messages)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='kafka_instance',
        usage='foo bars',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('num_brokers', type=int, default=3,
                        help='Numbers of brokers in the cluster.')
    parser.add_argument('--download-dir', type=str, default='/tmp/kafka-bin',
                        help='Download destination for Kafka')
    parser.add_argument('--kafka-version', type=str, default='0.8.2.1',
                        help='Kafka version to download')
    args = parser.parse_args()

    _exiting = False
    def _catch_sigint(signum, frame):
        global _exiting
        _exiting = True
        print('SIGINT received.')
    signal.signal(signal.SIGINT, _catch_sigint)

    cluster = KafkaInstance(num_instances=args.num_brokers,
                            kafka_version=args.kafka_version,
                            bin_dir=args.download_dir)
    print('Cluster started.')
    print('Brokers: {brokers}'.format(brokers=cluster.brokers))
    print('Zookeeper: {zk}'.format(zk=cluster.zookeeper))
    print('Waiting for SIGINT to exit.')
    while True:
        if _exiting:
            print('Exiting.')
            sys.exit(0)
        time.sleep(1)
