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
from __future__ import print_function

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
{port_config}
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

_kafka_ssl_properties = """
listeners=PLAINTEXT://localhost:{port},SSL://localhost:{ssl_port}
ssl.keystore.location={keystore_path}
ssl.keystore.password={store_pass}
ssl.key.password={store_pass}
ssl.truststore.location={truststore_path}
ssl.truststore.password={store_pass}
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

    def __init__(self, bin_dir, brokers, zookeeper, brokers_ssl=None):
        """Create a connection to the cluster.

        :param bin_dir:   Location of downloaded kafka bin
        :param brokers:   Comma-separated list of brokers
        :param zookeeper: Connection straing for ZK
        :param brokers_ssl: Comma-separated list of hosts with ssl-ports
        """
        self._bin_dir = bin_dir
        self.brokers = brokers
        self.zookeeper = zookeeper

        self.brokers_ssl = brokers_ssl
        self.certs = CertManager(bin_dir) if brokers_ssl is not None else None

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
            if not topic.startswith(b'__'):  # leave internal topics alone
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
                 scala_version='2.11',
                 bin_dir='/tmp/kafka-bin',
                 name='kafka',
                 use_gevent=False):
        """Start kafkainstace with given settings"""
        self._num_instances = num_instances
        self._kafka_version = kafka_version
        self._scala_version = scala_version
        self._bin_dir = bin_dir
        self._processes = []
        self._broker_procs = []
        self._brokers_started = 0  # incremented by _start_broker
        self.zookeeper = None
        self.brokers = None
        self.brokers_ssl = None
        self.certs = self._gen_ssl_certs()
        # TODO: Need a better name so multiple can run at once.
        #       other ManagedInstances use things like 'name-port'
        ManagedInstance.__init__(self, name, use_gevent=use_gevent)
        self.connection = KafkaConnection(
            bin_dir, self.brokers, self.zookeeper, self.brokers_ssl)

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
        url_fmt = 'https://archive.apache.org/dist/kafka/{kafka_version}/kafka_{scala_version}-{kafka_version}.tgz'
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

    def _add_broker(self, broker_port):
        new_broker = "localhost:{}".format(broker_port)
        brokers = self.brokers.split(",") if self.brokers else []
        brokers.append(new_broker)
        self.brokers = ",".join(brokers)

    def _add_ssl_broker(self, ssl_broker_port):
        if ssl_broker_port:
            new_broker_ssl = "localhost:{}".format(ssl_broker_port)
            brokers_ssl = self.brokers_ssl.split(",") if self.brokers_ssl else []
            brokers_ssl.append(new_broker_ssl)
            self.brokers_ssl = ",".join(brokers_ssl)

    def _gen_ssl_certs(self):
        """Attempt generating ssl certificates for testing

        :returns: :class:`CertManager` or None upon failure
        """
        if self._kafka_version >= "0.9":  # no SSL support in earlier versions
            try:
                return CertManager(self._bin_dir)
            except:  # eg. because openssl or other tools not installed
                log.exception("Couldn't generate ssl certs:")

    def _start_process(self):
        """Start the instance processes"""
        self._init_dirs()
        self._download_kafka()

        # Start all relevant processes and save which ports they use
        zk_port = self._start_zookeeper()
        self.zookeeper = 'localhost:{port}'.format(port=zk_port)

        broker_ports, broker_ssl_ports = self._start_brokers()

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

    def _start_broker_proc(self, port, ssl_port=None):
        """Start a broker proc and maintain handlers

        Returns a proc handler for the new broker.
        """
        # make port config for new broker
        if self.certs is not None:
            port_config = _kafka_ssl_properties.format(
                port=port,
                ssl_port=ssl_port,
                keystore_path=self.certs.keystore,
                truststore_path=self.certs.truststore,
                store_pass=self.certs.broker_pass)
        else:
            port_config = "port={}".format(port)

        self._brokers_started += 1
        i = self._brokers_started

        # write conf file for the new broker
        conf = os.path.join(self._conf_dir,
                            'kafka_{instance}.properties'.format(instance=i))
        with open(conf, 'w') as f:
            f.write(_kafka_properties.format(
                broker_id=i,
                port_config=port_config,
                zk_connstr=self.zookeeper,
                data_dir=self._data_dir + '_{instance}'.format(instance=i),
            ))

        # start process and append to self._broker_procs
        binfile = os.path.join(self._bin_dir, 'bin/kafka-server-start.sh')
        logfile = os.path.join(self._log_dir, 'kafka_{instance}.log'.format(instance=i))
        new_proc = (utils.Popen(
            args=[binfile, conf],
            stderr=utils.STDOUT,
            stdout=open(logfile, 'w'),
            use_gevent=self.use_gevent
        ))
        self._broker_procs.append(new_proc)

        # add localhost:port to internal list of (ssl)brokers
        self._add_broker(port)
        self._add_ssl_broker(ssl_port)

        return new_proc


    def _start_brokers(self):
        """Start all brokers and return used ports."""
        ports = self._port_generator(9092)
        used_ports = []
        used_ssl_ports = []
        ssl_port = None
        for i in range(self._num_instances):
            port = next(ports)
            used_ports.append(port)
            log.info('Starting Kafka on port %i.', port)

            if self.certs is not None:
                ssl_port = next(ports)
                used_ssl_ports.append(ssl_port)  # to return at end
            self._start_broker_proc(port, ssl_port)

        return used_ports, used_ssl_ports

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


class CertManager(object):
    """Helper that generates key pairs/stores for brokers and clients

    NB: intended only for test suite purposes.  For instance, currently the
    same cert is shared between all brokers, and another between all clients,
    even between different instances of CertManager.  And for another, this
    class freely discloses passwords for its keys: they're purely here to
    test if the configuration with passwords works
    """

    def __init__(self, root_dir):
        # It would be more elegant to use a tempdir here, but that would make
        # it difficult for KafkaConnection to find the CA cert for an already
        # running cluster, and so we just use a fixed location:
        self._dir = os.path.join(root_dir, "test-certs")

        self.validity_days = '10000'

        self.root_cert = os.path.join(self._dir, "root.cert")
        self.root_key = os.path.join(self._dir, "root.key")
        self.root_pass = "NfXI63pmNqbHF4tq"

        self.truststore = os.path.join(self._dir, "broker.truststore.jks")
        self.keystore = os.path.join(self._dir, "broker.keystore.jks")
        self.broker_pass = "JJQqPLUV1ZIFD6ny"

        self.client_cert = os.path.join(self._dir, "client.cert")
        self.client_key = os.path.join(self._dir, "client.key")
        self.client_pass = "dWJTxlilbJcH1Wgn"

        if not os.path.exists(self.root_cert):
            try:
                os.makedirs(self._dir)
            except OSError:
                pass
            self._gen_root_cert()
            self._gen_broker_truststore()
            self._gen_broker_keystore()
            self._gen_client_cert()

    def _gen_root_cert(self):
        """Generate root certificate"""
        cmd = ['openssl', 'req',
               '-x509',
               '-newkey', 'rsa:2048',
               '-subj', '/CN=ca.example.com',
               '-days', self.validity_days,
               '-out', self.root_cert,
               '-keyout', self.root_key,
               '-passout', 'env:ROOT_PASS']
        return subprocess.check_call(cmd, env=dict(ROOT_PASS=self.root_pass))

    def _gen_broker_truststore(self):
        """Generate truststore that trusts self.root_cert"""
        cmd = ['keytool', '-importcert', '-alias', 'root',
                                         '-file', self.root_cert,
                                         '-keystore', self.truststore,
                                         '-storepass:env', 'BROKER_PASS',
                                         '-noprompt']
        env = dict(BROKER_PASS=self.broker_pass)
        return subprocess.check_call(cmd, env=env)

    def _gen_broker_keystore(self):
        """Generate broker keypair, signed by self.root_cert"""
        env = dict(BROKER_PASS=self.broker_pass, ROOT_PASS=self.root_pass)
        csr_path = os.path.join(self._dir, "broker.csr")
        cert_path = os.path.join(self._dir, "broker.cert")

        cmd = ['keytool', '-genkeypair', '-alias', 'broker',
                                         '-dname', 'CN=localhost',
                                         '-validity', self.validity_days,
                                         '-keystore', self.keystore,
                                         '-storepass:env', 'BROKER_PASS',
                                         '-keypass:env', 'BROKER_PASS',
                                         '-noprompt']
        subprocess.check_call(cmd, env=env)
        cmd = ['keytool', '-certreq', '-alias', 'broker',
                                      '-file', csr_path,
                                      '-keystore', self.keystore,
                                      '-storepass:env', 'BROKER_PASS']
        subprocess.check_call(cmd, env=env)
        cmd = ['openssl', 'x509', '-req', '-in', csr_path,
                                          '-out', cert_path,
                                          '-days', self.validity_days,
                                          '-CA', self.root_cert,
                                          '-CAkey', self.root_key,
                                          '-CAcreateserial',
                                          '-passin', 'env:ROOT_PASS']
        subprocess.check_call(cmd, env=env)
        cmd = ['keytool', '-importcert', '-alias', 'root',
                                         '-file', self.root_cert,
                                         '-keystore', self.keystore,
                                         '-storepass:env', 'BROKER_PASS',
                                         '-noprompt']
        subprocess.check_call(cmd, env=env)
        cmd = ['keytool', '-importcert', '-alias', 'broker',
                                         '-file', cert_path,
                                         '-keystore', self.keystore,
                                         '-storepass:env', 'BROKER_PASS']
        subprocess.check_call(cmd, env=env)

        os.unlink(csr_path)
        os.unlink(cert_path)

    def _gen_client_cert(self):
        """Generate client keypair and sign it"""
        env = dict(CLIENT_PASS=self.client_pass, ROOT_PASS=self.root_pass)
        csr_path = os.path.join(self._dir, "client.csr")

        cmd = ['openssl', 'req', '-newkey', 'rsa:2048',
                                 '-subj', '/CN=localhost',
                                 '-days', self.validity_days,
                                 '-out', csr_path,
                                 '-keyout', self.client_key,
                                 '-passout', 'env:CLIENT_PASS']
        subprocess.check_call(cmd, env=env)
        cmd = ['openssl', 'x509', '-req', '-in', csr_path,
                                          '-out', self.client_cert,
                                          '-days', self.validity_days,
                                          '-CA', self.root_cert,
                                          '-CAkey', self.root_key,
                                          '-passin', 'env:ROOT_PASS']
        subprocess.check_call(cmd, env=env)

        os.unlink(csr_path)


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
    parser.add_argument('--scala-version', type=str, default='2.11',
                        help='Scala version for kafka build')
    parser.add_argument('--export-hosts', type=str,
                        help='Write host strings to given file path')
    args = parser.parse_args()

    _exiting = False
    def _catch_sigint(signum, frame):
        global _exiting
        _exiting = True
        print('SIGINT received.')
    signal.signal(signal.SIGINT, _catch_sigint)

    cluster = KafkaInstance(num_instances=args.num_brokers,
                            kafka_version=args.kafka_version,
                            scala_version=args.scala_version,
                            bin_dir=args.download_dir)
    print('Cluster started.')
    print('Brokers: {brokers}'.format(brokers=cluster.brokers))
    print('Zookeeper: {zk}'.format(zk=cluster.zookeeper))
    print('Waiting for SIGINT to exit.')

    if args.export_hosts is not None:
        with open(args.export_hosts, 'w') as f:
            f.write('BROKERS={}\n'.format(cluster.brokers))
            if cluster.brokers_ssl:
                f.write('BROKERS_SSL={}\n'.format(cluster.brokers_ssl))
            f.write('ZOOKEEPER={}\n'.format(cluster.zookeeper))

    while True:
        if _exiting:
            print('Exiting.')
            sys.exit(0)
        time.sleep(1)
