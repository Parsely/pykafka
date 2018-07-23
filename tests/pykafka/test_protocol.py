import operator
import unittest2

from pykafka import protocol
from pykafka.common import CompressionType
from pykafka.membershipprotocol import RangeProtocol
from pykafka.utils.compat import buffer


class TestMetadataAPI(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        req = protocol.MetadataRequest()
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                # header
                b'\x00\x00\x00\x15'  # len(buffer)
                b'\x00\x03'  # ApiKey
                b'\x00\x00'  # api version
                b'\x00\x00\x00\x00'  # correlation id
                b'\x00\x07'  # len(client id)
                    b'pykafka'  # client id  # noqa
                # end header

                b'\x00\x00\x00\x00'  # len(topics)
            )
        )

    def test_response(self):
        cluster = protocol.MetadataResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(brokers)
                    b'\x00\x00\x00\x00'  # node id # noqa
                    b'\x00\x09'  # len(host)
                        b'localhost'  # host
                    b'\x00\x00#\x84'  # port
                b'\x00\x00\x00\x01'  # len(topic metadata)
                    b'\x00\x00'  # error code
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00\x00\x00\x02'  # len(partition metadata)
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x00'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replica
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x01'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replicas
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
            )
        )
        self.assertEqual(cluster.brokers[0].host, b'localhost')
        self.assertEqual(cluster.brokers[0].port, 9092)
        self.assertEqual(cluster.topics[b'test'].partitions[0].leader,
                         cluster.brokers[0].id)
        self.assertEqual(cluster.topics[b'test'].partitions[0].replicas,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.topics[b'test'].partitions[0].isr,
                         [cluster.brokers[0].id])

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.MetadataResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(brokers)
                    b'\x00\x00\x00\x00'  # node is # noqa
                    b'\x00\x09'  # len(host)
                        b'localhost'  # host
                    b'\x00\x00#\x84'  # port
                b'\x00\x00\x00\x01'  # len(topic metadata)
                    b'\x00\x00'  # error code
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00\x00\x00\x02'  # len(partition metadata)
                        b'\x00\x03'  # partition error code
                            b'\x00\x00\x00\x00'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replica
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x01'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replica
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
            )
        )
        self.assertEqual(response.topics[b'test'].partitions[0].err, 3)

    def test_topic_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.MetadataResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(brokers)
                    b'\x00\x00\x00\x00'  # node id  # noqa
                    b'\x00\x09'  # len(host)
                        b'localhost'  # host
                    b'\x00\x00#\x84'  # port
                b'\x00\x00\x00\x01'  # len(topic metadata)
                    b'\x00\x03'  # error code
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00\x00\x00\x02'  # len(partition metadata)
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x00'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replica
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x01'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replicas
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
            )
        )
        self.assertEqual(response.topics[b'test'].err, 3)


class TestMetadataAPIV1(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        req = protocol.MetadataRequestV1()
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                # header
                b'\x00\x00\x00\x15'  # len(buffer)
                b'\x00\x03'  # ApiKey
                b'\x00\x01'  # api version
                b'\x00\x00\x00\x00'  # correlation id
                b'\x00\x07'  # len(client id)
                    b'pykafka'  # client id  # noqa
                # end header

                b'\xff\xff\xff\xff'  # len(topics)
            )
        )

    def test_response(self):
        cluster = protocol.MetadataResponseV1(
            buffer(
                b'\x00\x00\x00\x01'  # len(brokers)
                    b'\x00\x00\x00\x00'  # node id # noqa
                    b'\x00\x09'  # len(host)
                        b'localhost'  # host
                    b'\x00\x00#\x84'  # port
                    b'\xff\xff'  # len(rack)
                b'\x00\x00\x00\x00'  # controller_id
                b'\x00\x00\x00\x01'  # len(topic metadata)
                    b'\x00\x00'  # error code
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00'  # is_internal
                    b'\x00\x00\x00\x02'  # len(partition metadata)
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x00'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replica
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x01'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replicas
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
            )
        )
        self.assertEqual(cluster.brokers[0].host, b'localhost')
        self.assertEqual(cluster.brokers[0].port, 9092)
        self.assertEqual(cluster.topics[b'test'].partitions[0].leader,
                         cluster.brokers[0].id)
        self.assertEqual(cluster.topics[b'test'].partitions[0].replicas,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.topics[b'test'].partitions[0].isr,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.brokers[0].rack, None)
        self.assertEqual(cluster.controller_id, 0)
        self.assertEqual(cluster.topics[b'test'].is_internal, False)


class TestMetadataAPIV2(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        req = protocol.MetadataRequestV2()
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                # header
                b'\x00\x00\x00\x15'  # len(buffer)
                b'\x00\x03'  # ApiKey
                b'\x00\x02'  # api version
                b'\x00\x00\x00\x00'  # correlation id
                b'\x00\x07'  # len(client id)
                    b'pykafka'  # client id  # noqa
                # end header

                b'\xff\xff\xff\xff'  # len(topics)
            )
        )

    def test_response(self):
        cluster = protocol.MetadataResponseV2(
            buffer(
                b'\x00\x00\x00\x01'  # len(brokers)
                    b'\x00\x00\x00\x00'  # node id # noqa
                    b'\x00\x09'  # len(host)
                        b'localhost'  # host
                    b'\x00\x00#\x84'  # port
                    b'\xff\xff'  # len(rack)
                b'\x00\x01'  # len(cluster_id)
                    b'a'  # cluster_id
                b'\x00\x00\x00\x00'  # controller_id
                b'\x00\x00\x00\x01'  # len(topic metadata)
                    b'\x00\x00'  # error code
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00'  # is_internal
                    b'\x00\x00\x00\x02'  # len(partition metadata)
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x00'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replica
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x01'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replicas
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
            )
        )
        self.assertEqual(cluster.brokers[0].host, b'localhost')
        self.assertEqual(cluster.brokers[0].port, 9092)
        self.assertEqual(cluster.topics[b'test'].partitions[0].leader,
                         cluster.brokers[0].id)
        self.assertEqual(cluster.topics[b'test'].partitions[0].replicas,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.topics[b'test'].partitions[0].isr,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.brokers[0].rack, None)
        self.assertEqual(cluster.controller_id, 0)
        self.assertEqual(cluster.cluster_id, b"a")
        self.assertEqual(cluster.topics[b'test'].is_internal, False)


class TestMetadataAPIV3(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        req = protocol.MetadataRequestV3()
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                # header
                b'\x00\x00\x00\x15'  # len(buffer)
                b'\x00\x03'  # ApiKey
                b'\x00\x03'  # api version
                b'\x00\x00\x00\x00'  # correlation id
                b'\x00\x07'  # len(client id)
                    b'pykafka'  # client id  # noqa
                # end header

                b'\xff\xff\xff\xff'  # len(topics)
            )
        )

    def test_response(self):
        cluster = protocol.MetadataResponseV3(
            buffer(
                b'\x00\x00\x00\x00'  # throttle_time_ms
                b'\x00\x00\x00\x01'  # len(brokers)
                    b'\x00\x00\x00\x00'  # node id # noqa
                    b'\x00\x09'  # len(host)
                        b'localhost'  # host
                    b'\x00\x00#\x84'  # port
                    b'\xff\xff'  # len(rack)
                b'\x00\x01'  # len(cluster_id)
                    b'a'  # cluster_id
                b'\x00\x00\x00\x00'  # controller_id
                b'\x00\x00\x00\x01'  # len(topic metadata)
                    b'\x00\x00'  # error code
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00'  # is_internal
                    b'\x00\x00\x00\x02'  # len(partition metadata)
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x00'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replica
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x01'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replicas
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
            )
        )
        self.assertEqual(cluster.brokers[0].host, b'localhost')
        self.assertEqual(cluster.brokers[0].port, 9092)
        self.assertEqual(cluster.topics[b'test'].partitions[0].leader,
                         cluster.brokers[0].id)
        self.assertEqual(cluster.topics[b'test'].partitions[0].replicas,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.topics[b'test'].partitions[0].isr,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.brokers[0].rack, None)
        self.assertEqual(cluster.throttle_time_ms, 0)
        self.assertEqual(cluster.controller_id, 0)
        self.assertEqual(cluster.cluster_id, b"a")
        self.assertEqual(cluster.topics[b'test'].is_internal, False)


class TestMetadataAPIV4(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        req = protocol.MetadataRequestV4()
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                # header
                b'\x00\x00\x00\x16'  # len(buffer)
                b'\x00\x03'  # ApiKey
                b'\x00\x04'  # api version
                b'\x00\x00\x00\x00'  # correlation id
                b'\x00\x07'  # len(client id)
                    b'pykafka'  # client id  # noqa
                # end header

                b'\xff\xff\xff\xff'  # len(topics)
                b'\x01'  # allow_topic_autocreation
            )
        )

    def test_response(self):
        cluster = protocol.MetadataResponseV4(
            buffer(
                b'\x00\x00\x00\x00'  # throttle_time_ms
                b'\x00\x00\x00\x01'  # len(brokers)
                    b'\x00\x00\x00\x00'  # node id # noqa
                    b'\x00\x09'  # len(host)
                        b'localhost'  # host
                    b'\x00\x00#\x84'  # port
                    b'\xff\xff'  # len(rack)
                b'\x00\x01'  # len(cluster_id)
                    b'a'  # cluster_id
                b'\x00\x00\x00\x00'  # controller_id
                b'\x00\x00\x00\x01'  # len(topic metadata)
                    b'\x00\x00'  # error code
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00'  # is_internal
                    b'\x00\x00\x00\x02'  # len(partition metadata)
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x00'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replica
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x01'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replicas
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
            )
        )
        self.assertEqual(cluster.brokers[0].host, b'localhost')
        self.assertEqual(cluster.brokers[0].port, 9092)
        self.assertEqual(cluster.topics[b'test'].partitions[0].leader,
                         cluster.brokers[0].id)
        self.assertEqual(cluster.topics[b'test'].partitions[0].replicas,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.topics[b'test'].partitions[0].isr,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.brokers[0].rack, None)
        self.assertEqual(cluster.throttle_time_ms, 0)
        self.assertEqual(cluster.controller_id, 0)
        self.assertEqual(cluster.cluster_id, b"a")
        self.assertEqual(cluster.topics[b'test'].is_internal, False)


class TestMetadataAPIV5(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        req = protocol.MetadataRequestV5()
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                # header
                b'\x00\x00\x00\x16'  # len(buffer)
                b'\x00\x03'  # ApiKey
                b'\x00\x05'  # api version
                b'\x00\x00\x00\x00'  # correlation id
                b'\x00\x07'  # len(client id)
                    b'pykafka'  # client id  # noqa
                # end header

                b'\xff\xff\xff\xff'  # len(topics)
                b'\x01'  # allow_topic_autocreation
            )
        )

    def test_response(self):
        cluster = protocol.MetadataResponseV5(
            buffer(
                b'\x00\x00\x00\x00'  # throttle_time_ms
                b'\x00\x00\x00\x01'  # len(brokers)
                    b'\x00\x00\x00\x00'  # node id # noqa
                    b'\x00\x09'  # len(host)
                        b'localhost'  # host
                    b'\x00\x00#\x84'  # port
                    b'\xff\xff'  # len(rack)
                b'\x00\x01'  # len(cluster_id)
                    b'a'  # cluster_id
                b'\x00\x00\x00\x00'  # controller_id
                b'\x00\x00\x00\x01'  # len(topic metadata)
                    b'\x00\x00'  # error code
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00'  # is_internal
                    b'\x00\x00\x00\x02'  # len(partition metadata)
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x00'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replica
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
                            b'\x00\x00\x00\x01'  # len(offline_replicas)
                                b'\x00\x00\x00\x00'  # offline_replicas
                        b'\x00\x00'  # partition error code
                            b'\x00\x00\x00\x01'  # partition id
                            b'\x00\x00\x00\x00'  # leader
                            b'\x00\x00\x00\x01'  # len(replicas)
                                b'\x00\x00\x00\x00'  # replicas
                            b'\x00\x00\x00\x01'  # len(isr)
                                b'\x00\x00\x00\x00'  # isr
                            b'\x00\x00\x00\x01'  # len(offline_replicas)
                                b'\x00\x00\x00\x00'  # offline_replicas
            )
        )
        self.assertEqual(cluster.brokers[0].host, b'localhost')
        self.assertEqual(cluster.brokers[0].port, 9092)
        self.assertEqual(cluster.topics[b'test'].partitions[0].leader,
                         cluster.brokers[0].id)
        self.assertEqual(cluster.topics[b'test'].partitions[0].replicas,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.topics[b'test'].partitions[0].isr,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.topics[b'test'].partitions[0].offline_replicas,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.brokers[0].rack, None)
        self.assertEqual(cluster.throttle_time_ms, 0)
        self.assertEqual(cluster.controller_id, 0)
        self.assertEqual(cluster.cluster_id, b"a")
        self.assertEqual(cluster.topics[b'test'].is_internal, False)


class TestProduceAPI(unittest2.TestCase):
    maxDiff = None

    test_messages = [
        protocol.Message(b'this is a test message', partition_key=b'asdf'),
        protocol.Message(b'this is a test message', partition_key=b'asdf',
                         timestamp=1497302164, protocol_version=1),
        protocol.Message(b'this is also a test message', partition_key=b'test_key'),
        protocol.Message(b"this doesn't have a partition key"),
    ]

    def test_request(self):
        message = self.test_messages[0]
        req = protocol.ProduceRequest()
        req.add_message(message, b'test', 0)
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b"\x00\x00\x00a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07pykafka"  # header
                b'\x00\x01'  # required acks
                b'\x00\x00\'\x10'  # timeout
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len (partitions)
                        b'\x00\x00\x00\x00'  # partition
                        b'\x00\x00\x004'  # message set size
                            b'\xff\xff\xff\xff\xff\xff\xff\xff'  # offset
                            b'\x00\x00\x00('  # message size
                                b'\x0e\x8a\x19O'  # crc
                                b'\x00'  # magic byte
                                b'\x00'  # attributes
                                b'\x00\x00\x00\x04'  # len(key)
                                    b'asdf'  # key
                                b'\x00\x00\x00\x16'  # len(value)
                                    b"this is a test message"  # value
            )
        )

    def test_request_message_timestamp(self):
        message = self.test_messages[1]
        req = protocol.ProduceRequest()
        req.add_message(message, b'test', 0)
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00i\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\x01'  # required acks
                b"\x00\x00\'\x10"  # timeout
                b"\x00\x00\x00\x01"  # len(topics)
                    b"\x00\x04"  # len(topic name) # noqa
                        b"test"  # topic name
                    b"\x00\x00\x00\x01"  # len(partitions)
                        b"\x00\x00\x00\x00"  # partition
                        b"\x00\x00\x00<"  # message set size
                            b"\xff\xff\xff\xff\xff\xff\xff\xff"  # offset
                            b"\x00\x00\x000"  # message size
                                b"Z\x92\x80\t"  # crc
                                b"\x01"  # magic byte (protocol version)
                                b"\x00"  # attributes
                                b"\x00\x00\x00\x00Y?\x04\x94"  # timestamp
                                b"\x00\x00\x00\x04"  # len(key)
                                    b"asdf"  # key
                                b"\x00\x00\x00\x16"  # len(value)
                                    b"this is a test message"  # value
            )
        )

    def test_gzip_compression(self):
        req = protocol.ProduceRequest(compression_type=CompressionType.GZIP)
        [req.add_message(m, b'test_gzip', 0) for m in self.test_messages]
        msg = req.get_bytes()
        self.assertEqual(len(msg), 230)  # this isn't a good test

    def test_snappy_compression(self):
        req = protocol.ProduceRequest(compression_type=CompressionType.SNAPPY)
        [req.add_message(m, b'test_snappy', 0) for m in self.test_messages]
        msg = req.get_bytes()
        self.assertEqual(len(msg), 240)  # this isn't a good test

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.ProduceResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
                            b'\x00\x03'  # error code
                            b'\x00\x00\x00\x00\x00\x00\x00\x02'  # offset
            )
        )
        self.assertEqual(response.topics[b'test'][0].err, 3)

    def test_response(self):
        response = protocol.ProduceResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
                            b'\x00\x00'  # error code
                            b'\x00\x00\x00\x00\x00\x00\x00\x02'  # offset
            )
        )
        self.assertEqual(
            response.topics,
            {b'test': {0: protocol.ProducePartitionResponse(0, 2)}}
        )


class _FetchAPITestBase(object):
    maxDiff = None

    def msg_to_dict(self, msg):
        """Helper to extract data from Message slots"""
        attr_names = protocol.Message.__slots__
        f = operator.attrgetter(*attr_names)
        return dict(zip(attr_names, f(msg)))


class TestFetchAPI(unittest2.TestCase, _FetchAPITestBase):
    def _get_expected(self):
        return [{
            'partition_key': b'asdf',
            'compression_type': 0,
            'value': b'this is a test message',
            'offset': 0,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'timestamp': 0,
            'protocol_version': 0,
            'partition': None
        }, {
            'partition_key': b'test_key',
            'compression_type': 0,
            'value': b'this is also a test message',
            'offset': 1,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'timestamp': 0,
            'protocol_version': 0,
            'partition': None
        }, {
            'partition_key': None,
            'compression_type': 0,
            'value': b"this doesn't have a partition key",
            'offset': 2,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'timestamp': 0,
            'protocol_version': 0,
            'partition': None
        }]

    def test_request(self):
        preq = protocol.PartitionFetchRequest(b'test', 0, 1)
        req = protocol.FetchRequest(partition_requests=[preq, ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00;\x00\x01\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\xff\xff\xff\xff'  # replica id
                b'\x00\x00\x03\xe8'  # max wait time
                b'\x00\x00\x04\x00'  # min bytes
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
                            b'\x00\x00\x00\x00\x00\x00\x00\x01'  # fetch offset
                            b'\x00\x10\x00\x00'  # max bytes
            )
        )

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.FetchResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # len(topic)
                b'\x00\x00\x00\x01'  # len (partitions)
                    b'\x00\x00\x00\x00'  # partition id
                    b'\x00\x03'  # error code
                        b'\x00\x00\x00\x00\x00\x00\x00\x02'  # highwater mark offset
                        b'\x00\x00\x00B'  # message set size
                            b'\x00\x00\x00\x00\x00\x00\x00\x01'  # offset
                            b'\x00\x00\x006'  # message size
                                b'\xa3 ^B'  # crc
                                b'\x00'  # magic byte
                                b'\x00'  # attributes
                                b'\x00\x00\x00\x12'  # len(key)
                                    b'test_partition_key'  # key
                                b'\x00\x00\x00\x16'  # len(value)
                                    b'this is a test message'  # value
            )
        )
        self.assertEqual(response.topics[b'test'][0].err, 3)

    def test_response(self):
        resp = protocol.FetchResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # len(topic)
                b'\x00\x00\x00\x01'  # len (partitions)
                    b'\x00\x00\x00\x00'  # partition id
                    b'\x00\x00'  # error code
                        b'\x00\x00\x00\x00\x00\x00\x00\x02'  # highwater mark offset
                        b'\x00\x00\x00B'  # message set size
                            b'\x00\x00\x00\x00\x00\x00\x00\x01'  # offset
                            b'\x00\x00\x006'  # message size
                                b'\xa3 ^B'  # crc
                                b'\x00'  # magic byte
                                b'\x00'  # attributes
                                b'\x00\x00\x00\x12'  # len(key)
                                    b'test_partition_key'  # key
                                b'\x00\x00\x00\x16'  # len(value)
                                    b'this is a test message'  # value
            )
        )
        self.assertEqual(len(resp.topics[b'test'][0].messages), 1)
        self.assertEqual(resp.topics[b'test'][0].max_offset, 2)
        message = resp.topics[b'test'][0].messages[0]
        self.assertEqual(message.value, b'this is a test message')
        self.assertEqual(message.partition_key, b'test_partition_key')
        self.assertEqual(message.compression_type, 0)
        self.assertEqual(message.offset, 1)

    def test_gzip_decompression(self):
        msg = b''.join([
            b"\x00\x00\x00\x01\x00\ttest_gzip\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"
            b"\x00\x00\x00\x00\x00\x05\x00\x00\x00\xad\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00"
            b"\x00\xa1S\x82\x9d\xff\x00\x01\xff\xff\xff\xff\x00\x00\x00\x93\x1f\x8b\x08\x00\x00"
            b"\x00\x00\x00\x00\x00c`\x80\x03\r\xbe.I\x7f0\x8b%\xb18%\rH\x8b\x95dd\x16+\x00Q\xa2"
            b"BIjq\x89Bnjqqbz*T=#\x10\x1b\xb2\xf3\xcb\xf4\x81y\x1c \x15\xf1\xd9\xa9\x95@\xb6"
            b"4\\_Nq>v\xcdL@\xac\x7f\xb5(\xd9\x98\x81\xe1?\x10\x00y\x8a`M)\xf9\xa9\xc5y\xea%\n"
            b"\x19\x89e\xa9@\x9d\x05\x89E%\x99%\x99\xf9y\n\x10\x93A\x80\x19\x88\xcd/\x9a<1\xc5"
            b"\xb0\x97h#X\xc8\xb0\x1d\x00Bj\t\\*\x01\x00\x00\x00\x00\x00\x00"
        ])
        response = protocol.FetchResponse(msg)
        expected_data = self._get_expected()
        for i in range(len(expected_data)):
            self.assertDictEqual(
                self.msg_to_dict(response.topics[b'test_gzip'][0].messages[i]),
                expected_data[i])

    def test_snappy_decompression(self):
        msg = b''.join([
            b'\x00\x00\x00\x01'  # len(topics)
            b'\x00\x0b'  # len(topic name)
                b'test_snappy'  # topic name  # noqa
            b'\x00\x00\x00\x01'  # len(partitions)
                b'\x00\x00\x00\x00'  # partition
                    b'\x00\x00'  # error code
                    b'\x00\x00\x00\x00\x00\x00\x00\x03'  # highwater mark offset
                    b'\x00\x00\x00\xb5'  # message set size
                        b'\x00\x00\x00\x00\x00\x00\x00\x02'  # offset
                        b'\x00\x00\x00\xa9'  # message size
                            b'\xc1\xf2\xa3\xe1'  # crc
                            b'\x00'  # magic bytes
                            b'\x02'  # attributes
                            b'\xff\xff\xff\xff'  # len(key)
                            b'\x00\x00\x00\x9b'  # len(value)
                                b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x87\xac\x01\x00\x00\x19\x01\x10(\x0e\x8a\x19O\x05\x0fx\x04asdf\x00\x00\x00\x16this is a test message\x05$(\x00\x00\x01\x00\x00\x001\x07\x0f\x1c\x8e\x05\x10\x00\x08\x01"\x1c_key\x00\x00\x00\x1b\x158\x08lsoV=\x00H\x02\x00\x00\x00/\xd5rc3\x00\x00\xff\xff\xff\xff\x00\x00\x00!\x055ldoesn\'t have a partition key'  # value
        ])
        response = protocol.FetchResponse(msg)
        expected_data = self._get_expected()
        for i in range(len(expected_data)):
            self.assertDictEqual(
                self.msg_to_dict(response.topics[b'test_snappy'][0].messages[i]),
                expected_data[i])


class TestFetchAPIV1(unittest2.TestCase, _FetchAPITestBase):
    RESPONSE_CLASS = protocol.FetchResponseV1

    def _get_expected(self):
        return [{
            'partition_key': b'asdf',
            'compression_type': 0,
            'value': b'this is a test message',
            'offset': 0,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'timestamp': 1497393304998,
            'protocol_version': 1,
            'partition': None
        }, {
            'partition_key': b'test_key',
            'compression_type': 0,
            'value': b'this is also a test message',
            'offset': 1,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'timestamp': 1497393305005,
            'protocol_version': 1,
            'partition': None
        }, {
            'partition_key': None,
            'compression_type': 0,
            'value': b"this doesn't have a partition key",
            'offset': 2,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'timestamp': 1497393305013,
            'protocol_version': 1,
            'partition': None
        }, {
            'partition_key': b"test_key",
            'compression_type': 0,
            'value': b"this has a partition key and a timestamp",
            'offset': 3,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'timestamp': 1497302164,
            'protocol_version': 1,
            'partition': None
        }, {
            'partition_key': None,
            'compression_type': 0,
            'value': b"this has a timestamp",
            'offset': 4,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'timestamp': 1497302164,
            'protocol_version': 1,
            'partition': None
        }]

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = self.RESPONSE_CLASS(
            buffer(
                b'\x00\x00\x00\x01'  # throttle time
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # len(topic)
                b'\x00\x00\x00\x01'  # len (partitions)
                    b'\x00\x00\x00\x00'  # partition id
                    b'\x00\x03'  # error code
                        b'\x00\x00\x00\x00\x00\x00\x00\x02'  # highwater mark offset
                        b'\x00\x00\x00B'  # message set size
                            b'\x00\x00\x00\x00\x00\x00\x00\x01'  # offset
                            b'\x00\x00\x006'  # message size
                                b'\xa3 ^B'  # crc
                                b'\x00'  # magic byte
                                b'\x00'  # attributes
                                b'\x00\x00\x00\x12'  # len(key)
                                    b'test_partition_key'  # key
                                b'\x00\x00\x00\x16'  # len(value)
                                    b'this is a test message'  # value
            )
        )
        self.assertEqual(response.topics[b'test'][0].err, 3)

    def test_response(self):
        resp = self.RESPONSE_CLASS(
            buffer(
                b'\x00\x00\x00\x01'  # throttle time
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # len(topic)
                b'\x00\x00\x00\x01'  # len (partitions)
                    b'\x00\x00\x00\x00'  # partition id
                    b'\x00\x00'  # error code
                        b'\x00\x00\x00\x00\x00\x00\x00\x02'  # highwater mark offset
                        b'\x00\x00\x00B'  # message set size
                            b'\x00\x00\x00\x00\x00\x00\x00\x01'  # offset
                            b'\x00\x00\x006'  # message size
                                b'\xa3 ^B'  # crc
                                b'\x00'  # magic byte
                                b'\x00'  # attributes
                                b'\x00\x00\x00\x12'  # len(key)
                                    b'test_partition_key'  # key
                                b'\x00\x00\x00\x16'  # len(value)
                                    b'this is a test message'  # value
            )
        )
        self.assertEqual(resp.throttle_time, 1)
        self.assertEqual(len(resp.topics[b'test'][0].messages), 1)
        self.assertEqual(resp.topics[b'test'][0].max_offset, 2)
        message = resp.topics[b'test'][0].messages[0]
        self.assertEqual(message.value, b'this is a test message')
        self.assertEqual(message.partition_key, b'test_partition_key')
        self.assertEqual(message.compression_type, 0)
        self.assertEqual(message.offset, 1)

    def test_gzip_decompression(self):
        msg = b''.join([
b'\x00\x00\x00\x00\x00\x00\x00\x01\x00\x0btest_gzip_5\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x02\x17\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Z\xb1Z\xf4\xc3\x01\x01\x00\x00\x01\\\xa3\x98\x95\xa6\xff\xff\xff\xff\x00\x00\x00D\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00c`\x80\x03\x03\x97?.{\x19\x81\x0c\xc6\x98\xc53\xa6.\x032X\x12\x8bS\xd2\x80\xb4XIFf\xb1\x02\x10%*\x94\xa4\x16\x97(\xe4\xa6\x16\x17\'\xa6\xa7\x02\x00N\xddm\x92<\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00`\xcdX\t\xed\x01\x01\x00\x00\x01\\\xa3\x98\x95\xad\xff\xff\xff\xff\x00\x00\x00J\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00c`\x80\x03\xcb%\xf2\x01\x99\x8c@\x06c\xcc\xe2\x19S\xd7\x02\x19\x1c%\xa9\xc5%\xf1\xd9\xa9\x95@\xb6tIFf\xb1\x02\x10%\xe6\x14\xe7+$*\x80\xa4\x14rS\x8b\x8b\x13\xd3S\x01\xfe<~BE\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00b\xba\xb0lN\x01\x01\x00\x00\x01\\\xa3\x98\x95\xb5\xff\xff\xff\xff\x00\x00\x00L\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00c`\x80\x03s\xfb\xad2\x07\x19\x81\x0c\xc6\x98\xc53\xa6n\xfd\x0f\x04@\x8ebIFf\xb1BJ~jq\x9ez\x89BFbY\xaaB\xa2BAbQIfIf~\x9eBvj%\x00\xc8f?\xe3C\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00l\x9f\xd7)\xa0\x01\x01\x00\x00\x00\x00Y?\x04\x94\xff\xff\xff\xff\x00\x00\x00V\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00c`\x80\x037\x86wK\x8f0\x82\x99\x91\xf6,S\x80\x14GIjqI|vj%\x90\xadQ\x92\x91Y\xac\x90\x91X\xac\x90\xa8P\x90XT\x92Y\x92\x99\x9f\xa7\x00\x94SH\xccK\x01\x8a\x95d\xe6\x02\x15\'\xe6\x16\x00\x00\xac\xc0O.R\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00S\xd8"\xff7\x01\x01\x00\x00\x00\x00Y?\x04\x94\xff\xff\xff\xff\x00\x00\x00=\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00c`\x80\x03\xad\x0f\xae\x97^2\x82\x99\x91\xf6,S\xfe\x03\x01\x90)R\x92\x91Y\xac\x90\x91X\xac\x90\xa8P\x92\x99\x9bZ\\\x92\x98[\x00\x00\x83\x0f\xe5\xc16\x00\x00\x00\x00\x00\x00\x00' # noqa
            ])
        response = self.RESPONSE_CLASS(msg)
        expected_data = self._get_expected()
        for i in range(len(expected_data)):
            self.assertDictEqual(
                self.msg_to_dict(response.topics[b'test_gzip_5'][0].messages[i]),
                expected_data[i])

    def test_snappy_decompression(self):
        msg = b''.join([
b"\x00\x00\x00\x00\x00\x00\x00\x01\x00\x0btest_snappy\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x02=\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00a\xaa\x92\x19\xe2\x01\x02\x00\x00\x01\\\xa3\xa5\xab/\xff\xff\xff\xff\x00\x00\x00K\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x007<\x00\x00\x19\x01\xc00\x86\xf55\x85\x01\x00\x00\x00\x01\\\xa3\xa5\xab/\x00\x00\x00\x04asdf\x00\x00\x00\x16this is a test message\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00jI\xfc\xe7\xf6\x01\x02\x00\x00\x01\\\xa3\xa5\xab\xa1\xff\xff\xff\xff\x00\x00\x00T\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00@E\x00\x00\x19\x01\xe49\xd4\r\xf8v\x01\x00\x00\x00\x01\\\xa3\xa5\xab\xa1\x00\x00\x00\x08test_key\x00\x00\x00\x1bthis is also a test message\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00h\x80\t0\x95\x01\x02\x00\x00\x01\\\xa3\xa5\xab\xe1\xff\xff\xff\xff\x00\x00\x00R\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00>C\x00\x00\x19\x01\xdc72\x98C\xe8\x01\x00\x00\x00\x01\\\xa3\xa5\xab\xe1\xff\xff\xff\xff\x00\x00\x00!this doesn't have a partition key\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00uJ\xab\xd0\xdf\x01\x02\x00\x00\x00\x00Y?\x04\x94\xff\xff\xff\xff\x00\x00\x00_\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00KR\x00\x00\x19\x01\x14F\x00\xee\xa5\xc4\x01\x05\x10\xecY?\x04\x94\x00\x00\x00\x08test_key\x00\x00\x00(this has a partition key and a timestamp\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00Y\xfb!\x15\xce\x01\x02\x00\x00\x00\x00Y?\x04\x94\xff\xff\xff\xff\x00\x00\x00C\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00/6\x00\x00\x19\x01\x14*\xf0E\xd2\xe9\x01\x05\x10|Y?\x04\x94\xff\xff\xff\xff\x00\x00\x00\x14this has a timestamp\x00\x00\x00\x00" # noqa
        ])
        response = self.RESPONSE_CLASS(msg)
        expected_data = self._get_expected()
        for i in range(len(expected_data)):
            returned = self.msg_to_dict(response.topics[b'test_snappy'][0].messages[i])
            expected = expected_data[i]
            # ignore timestamps if they were auto-set
            if i <= 2:
                returned.pop("timestamp")
                expected.pop("timestamp")
            self.assertDictEqual(returned, expected)


class TestFetchAPIV2(TestFetchAPIV1):
    RESPONSE_CLASS = protocol.FetchResponseV2


class TestListOffsetAPI(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        preq = protocol.PartitionOffsetRequest(b'test', 0, -1, 1)
        req = protocol.ListOffsetRequest(partition_requests=[preq, ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x003\x00\x02\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\xff\xff\xff\xff'  # replica id
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
                        b'\xff\xff\xff\xff\xff\xff\xff\xff'  # time
                        b'\x00\x00\x00\x01'  # max number of offsets
            )
        )

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.ListOffsetResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partitoin
                        b'\x00\x03'  # error code
                        b'\x00\x00\x00\x01'  # len(offsets)
                            b'\x00\x00\x00\x00\x00\x00\x00\x02'  # offset
            )
        )
        self.assertEqual(response.topics[b'test'][0].err, 3)

    def test_response(self):
        resp = protocol.ListOffsetResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partitoin
                        b'\x00\x00'  # error code
                        b'\x00\x00\x00\x01'  # len(offsets)
                            b'\x00\x00\x00\x00\x00\x00\x00\x02'  # offset
            )
        )
        self.assertEqual(resp.topics[b'test'][0].offset, [2])


class TestListOffsetAPIV1(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        preq = protocol.PartitionOffsetRequest(b'test', 0, -1, 1)
        req = protocol.ListOffsetRequestV1(partition_requests=[preq, ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                # header
                b'\x00\x00\x00/'  # len(buffer)
                b'\x00\x02'  # ApiKey
                b'\x00\x01'  # api version
                b'\x00\x00\x00\x00'  # correlation id
                b'\x00\x07'  # len(client id)
                    b'pykafka'  # client id  # noqa
                # end header
                b'\xff\xff\xff\xff'  # replica id
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
                        b'\xff\xff\xff\xff\xff\xff\xff\xff'  # time
            )
        )

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.ListOffsetResponseV1(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partitoin
                        b'\x00\x03'  # error code
                        b'\x00\x00\x00\x00\x00\x00\x00\x02'  # timestamp
                        b'\x00\x00\x00\x01'  # len(offsets)
                            b'\x00\x00\x00\x00\x00\x00\x00\x02'  # offset
            )
        )
        self.assertEqual(response.topics[b'test'][0].err, 3)

    def test_response(self):
        resp = protocol.ListOffsetResponseV1(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name) # noqa
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partitoin
                        b'\x00\x00'  # error code
                        b'\x00\x00\x00\x00\x00\x00\x00\x02'  # timestamp
                        b'\x00\x00\x00\x01'  # len(offsets)
                            b'\x00\x00\x00\x00\x00\x00\x00\x02'  # offset
            )
        )
        self.assertEqual(resp.topics[b'test'][0].offset, [2])
        self.assertEqual(resp.topics[b'test'][0].timestamp, 2)


class TestOffsetCommitFetchAPI(unittest2.TestCase):
    maxDiff = None

    def test_consumer_metadata_request(self):
        req = protocol.GroupCoordinatorRequest(b'test')
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00\x17\x00\n\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\x04'  # len(group id)
                    b'test'  # group id # noqa
            )
        )

    def test_consumer_metadata_response(self):
        response = protocol.GroupCoordinatorResponse(
            buffer(
                b'\x00\x00'  # error code
                b'\x00\x00\x00\x00'  # coordinator id
                b'\x00\r'  # len(coordinator host)
                    b'emmett-debian'  # coordinator host # noqa
                b'\x00\x00#\x84'  # coordinator port
            )
        )
        self.assertEqual(response.coordinator_id, 0)
        self.assertEqual(response.coordinator_host, b'emmett-debian')
        self.assertEqual(response.coordinator_port, 9092)

    def test_offset_commit_request(self):
        preq = protocol.PartitionOffsetCommitRequest(
            b'test', 0, 68, 1426632066, b'testmetadata')
        req = protocol.OffsetCommitRequest(
            b'test', 1, b'pykafka', partition_requests=[preq, ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00T\x00\x08\x00\x01\x00\x00\x00\x00\x00\x07pykafka'
                b'\x00\x04'  # len(consumer group id)
                    b'test'  # consumer group id # noqa
                b'\x00\x00\x00\x01'  # consumer group generation id
                b'\x00\x07'  # len(consumer id)
                    b'pykafka'  # consumer id
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                b'\x00\x00\x00\x01'  # len(partitions)
                    b'\x00\x00\x00\x00'  # partition
                        b'\x00\x00\x00\x00\x00\x00\x00D'  # offset
                        b'\x00\x00\x00\x00U\x08\xad\x82'  # timestamp
                        b'\x00\x0c'  # len(metadata)
                            b'testmetadata'  # metadata
            )
        )

    def test_offset_commit_response(self):
        response = protocol.OffsetCommitResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x0c'  # len(topic name) # noqa
                        b'emmett.dummy'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
                            b'\x00\x00'  # error code
            )
        )
        self.assertEqual(response.topics[b'emmett.dummy'][0].err, 0)

    def test_offset_fetch_request(self):
        preq = protocol.PartitionOffsetFetchRequest(b'testtopic', 0)
        req = protocol.OffsetFetchRequest(b'test', partition_requests=[preq, ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                # header
                b'\x00\x00\x00\x2e'  # len(buffer)
                b'\x00\x09'  # ApiKey
                b'\x00\x00'  # api version
                b'\x00\x00\x00\x00'  # correlation id
                b'\x00\x07'  # len(client id)
                    b'pykafka'  # client id
                # end header

                b'\x00\x04'  # len(consumer group)
                    b'test'  # consumer group # noqa
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x09'  # len(topic name)
                        b'testtopic'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
            )
        )

    def test_offset_fetch_response(self):
        response = protocol.OffsetFetchResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x0c'  # len(topic name) # noqa
                        b'emmett.dummy'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
                            b'\x00\x00\x00\x00\x00\x00\x00\x01'  # offset
                            b'\x00\x00'  # len(metadata)
                            b'\x00\x00'  # error code
            )
        )
        self.assertEqual(response.topics[b'emmett.dummy'][0].metadata, b'')
        self.assertEqual(response.topics[b'emmett.dummy'][0].offset, 1)


class TestOffsetCommitFetchAPIV2(unittest2.TestCase):
    maxDiff = None

    def test_offset_fetch_request(self):
        req = protocol.OffsetFetchRequestV2(b'test', partition_requests=[])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                # header
                b'\x00\x00\x00\x1b'  # len(buffer)
                b'\x00\x09'  # ApiKey
                b'\x00\x02'  # api version
                b'\x00\x00\x00\x00'  # correlation id
                b'\x00\x07'  # len(client id)
                    b'pykafka'  # client id
                # end header

                b'\x00\x04'  # len(consumer group)
                    b'test'  # consumer group # noqa
                b'\xff\xff\xff\xff'  # len(topics) = -1 for empty topics list
            )
        )

    def test_offset_fetch_response(self):
        response = protocol.OffsetFetchResponseV2(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x0c'  # len(topic name) # noqa
                        b'emmett.dummy'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
                            b'\x00\x00\x00\x00\x00\x00\x00\x01'  # offset
                            b'\x00\x00'  # len(metadata)
                            b'\x00\x00'  # error code
                b'\x00\x00'  # response level error_code
            )
        )
        self.assertEqual(response.topics[b'emmett.dummy'][0].metadata, b'')
        self.assertEqual(response.topics[b'emmett.dummy'][0].offset, 1)
        self.assertEqual(response.err, 0)


class TestGroupMembershipAPI(unittest2.TestCase):
    maxDiff = None

    def test_consumer_group_protocol_metadata(self):
        meta = protocol.ConsumerGroupProtocolMetadata()
        msg = meta.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00\x00'  # version
                b'\x00\x01'  # len(subscription)
                    b'\x00\n'  # len(topic name) # noqa
                        b'dummytopic'  # topic name
                    b'\x00\x00\x00\x0c'  # len(userdata)
                        b'testuserdata')  # userdata
        )

    def test_join_group_request(self):
        topic_name = b'abcdefghij'
        membership_protocol = RangeProtocol
        membership_protocol.metadata.topic_names = [topic_name]
        req = protocol.JoinGroupRequest(b'dummygroup', b'testmember', topic_name,
                                        membership_protocol)
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00h\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\n'  # len(groupid)
                    b'dummygroup'  # groupid # noqa
                b'\x00\x00u0'  # session timeout
                b'\x00\n'  # len(memberid)
                    b'testmember'  # memberid
                b'\x00\x08'  # len(protocol type)
                    b'consumer'  # protocol type
                b'\x00\x00\x00\x01'  # len(group protocols)
                    b'\x00\x05'  # len(protocol name)
                        b'range'  # protocol name
                    b'\x00\x00\x00"'  # len(protocol metadata)
                        b'\x00\x00\x00\x00\x00\x01\x00\nabcdefghij\x00\x00\x00\x0ctestuserdata'  # protocol metadata
            )
        )

    def test_join_group_response(self):
        response = protocol.JoinGroupResponse(
            bytearray(
                b'\x00\x00'  # error code
                b'\x00\x00\x00\x01'  # generation id
                b'\x00\x17'  # len (group protocol)
                    b'dummyassignmentstrategy'  # group protocol # noqa
                b'\x00,'  # len(leader id)
                    b'pykafka-b2361322-674c-4e26-9194-305962636e57'  # leader id
                b'\x00,'  # len(member id)
                    b'pykafka-b2361322-674c-4e26-9194-305962636e57'  # member id
                b'\x00\x00\x00\x01'  # leb(members)
                    b'\x00,'  # len(member id)
                        b'pykafka-b2361322-674c-4e26-9194-305962636e57'  # member id
                    b'\x00\x00\x00"'  # len(member metadata)
                        b'\x00\x00\x00\x00\x00\x01\x00\ndummytopic\x00\x00\x00\x0ctestuserdata\x00\x00\x00\x00'  # member metadata
            )
        )
        self.assertEqual(response.generation_id, 1)
        self.assertEqual(response.group_protocol, b'dummyassignmentstrategy')
        member_id = b'pykafka-b2361322-674c-4e26-9194-305962636e57'
        self.assertEqual(response.leader_id, member_id)
        self.assertEqual(response.member_id, member_id)
        self.assertTrue(member_id in response.members)
        metadata = response.members[member_id]
        self.assertEqual(metadata.version, 0)
        self.assertEqual(metadata.topic_names, [b"dummytopic"])
        self.assertEqual(metadata.user_data, b"testuserdata")

    def test_member_assignment_construction(self):
        assignment = protocol.MemberAssignment([(b"mytopic1", [3, 5, 7, 9]),
                                                (b"mytopic2", [2, 4, 6, 8])])
        msg = assignment.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x01'  # version
                b'\x00\x00\x00\x02'  # len(partition assignment)
                    b'\x00\x08'  # len(topic) # noqa
                        b'mytopic1'  # topic
                    b'\x00\x00\x00\x04'  # len(partitions)
                        b'\x00\x00\x00\x03'  # partition
                        b'\x00\x00\x00\x05'  # partition
                        b'\x00\x00\x00\x07'  # partition
                        b'\x00\x00\x00\t'  # partition
                    b'\x00\x08'  # len(topic)
                        b'mytopic2'  # topic
                    b'\x00\x00\x00\x04'  # len(partitions)
                        b'\x00\x00\x00\x02'  # partition
                        b'\x00\x00\x00\x04'  # partition
                        b'\x00\x00\x00\x06'  # partition
                        b'\x00\x00\x00\x08'  # partition
            )
        )

    def test_sync_group_request(self):
        req = protocol.SyncGroupRequest(
            b'dummygroup', 1, b'testmember1',
            [
                (b"a", protocol.MemberAssignment([(b"mytopic1", [3, 5, 7, 9]),
                                                  (b"mytopic2", [3, 5, 7, 9])])),
                (b"b", protocol.MemberAssignment([(b"mytopic1", [2, 4, 6, 8]),
                                                  (b"mytopic2", [2, 4, 6, 8])]))
            ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00\xc4\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\n'  # len(group id)
                    b'dummygroup'  # group id # noqa
                b'\x00\x00\x00\x01'  # generation id
                b'\x00\x0b'  # len(member id)
                    b'testmember1'  # member id
                b'\x00\x00\x00\x02'  # len(group assignment)
                    b'\x00\x01'  # len(member id)
                        b'a'  # member id
                    b'\x00\x00\x00B'  # len(member assignment)
                        b'\x00\x01\x00\x00\x00\x02\x00\x08mytopic1\x00\x00\x00\x04\x00\x00\x00\x03\x00\x00\x00\x05\x00\x00\x00\x07\x00\x00\x00\t\x00\x08mytopic2\x00\x00\x00\x04\x00\x00\x00\x03\x00\x00\x00\x05\x00\x00\x00\x07\x00\x00\x00\t'  # member assignment
                    b'\x00\x01'  # len(member id)
                        b'b'  # member id
                    b'\x00\x00\x00B'  # len(member assignment)
                        b'\x00\x01\x00\x00\x00\x02\x00\x08mytopic1\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x06\x00\x00\x00\x08\x00\x08mytopic2\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x06\x00\x00\x00\x08'  # member assignment
            )
        )

    def test_sync_group_response(self):
        response = protocol.SyncGroupResponse(
            bytearray(
                b'\x00\x00'  # error code
                b'\x00\x00\x00H'  # len(member assignment)
                    b'\x00\x01\x00\x00\x00\x01\x00\x14testtopic_replicated\x00\x00\x00\n\x00\x00\x00\x06\x00\x00\x00\x07\x00\x00\x00\x08\x00\x00\x00\t\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00\x04\x00\x00\x00\x05,pyk'  # member assignment # noqa
            )
        )
        self.assertEqual(response.error_code, 0)
        expected_assignment = [(b'testtopic_replicated', [6, 7, 8, 9, 0, 1, 2, 3, 4, 5])]
        self.assertEqual(response.member_assignment.partition_assignment,
                         expected_assignment)

    def test_heartbeat_request(self):
        req = protocol.HeartbeatRequest(b'dummygroup', 1, b'testmember')
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00-\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\n'  # len(group id)
                    b'dummygroup'  # group id # noqa
                b'\x00\x00\x00\x01'  # generation id
                b'\x00\n'  # len(member id)
                    b'testmember'  # member id
            )
        )

    def test_heartbeat_response(self):
        response = protocol.HeartbeatResponse(
            bytearray(
                b'\x00\x00'  # error code
            )
        )
        self.assertEqual(response.error_code, 0)

    def test_leave_group_request(self):
        req = protocol.LeaveGroupRequest(b'dummygroup', b'testmember')
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00)\x00\r\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\n'  # len(group id)
                    b'dummygroup'  # group id # noqa
                b'\x00\n'  # len(member id)
                    b'testmember'  # member id
            )
        )

    def test_leave_group_response(self):
        response = protocol.LeaveGroupResponse(
            bytearray(
                b'\x00\x00'  # error code
            )
        )
        self.assertEqual(response.error_code, 0)


class TestAdministrativeAPI(unittest2.TestCase):
    maxDiff = None

    def test_list_groups_request(self):
        req = protocol.ListGroupsRequest()
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00\x11\x00\x10\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
            )
        )

    def test_list_groups_response(self):
        response = protocol.ListGroupsResponse(
            bytearray(
                b'\x00\x00'  # error code
                b'\x00\x00\x00\x01'  # len(groups)
                    b'\x00\t'  # len(group_id) # noqa
                        b'testgroup'  # group_id
                    b'\x00\x08'  # len(protocol_type)
                        b'consumer'  # protocol_type
            )
        )
        self.assertEqual(len(response.groups), 1)
        group = response.groups[b"testgroup"]
        self.assertEqual(group.group_id, b"testgroup")
        self.assertEqual(group.protocol_type, b"consumer")

    def test_describe_groups_request(self):
        req = protocol.DescribeGroupsRequest([b'testgroup'])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00 \x00\x0f\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\x00\x00\x01'  # len(group_ids)
                    b'\x00\t'  # len(group_id) # noqa
                        b'testgroup'  # group_id
            )
        )

    def test_describe_groups_response(self):
        response = protocol.DescribeGroupsResponse(
            bytearray(
                b'\x00\x00\x00\x01'  # len(groups)
                    b'\x00\x00'  # error code # noqa
                    b'\x00\t'  # len(group_id)
                        b'testgroup'  # group_id
                    b'\x00\x06'  # len(state)
                        b'Stable'  # state
                    b'\x00\x08'  # len(protocol_type)
                        b'consumer'  # protocol_type
                    b'\x00\x05'  # len(protocol)
                        b'range'  # protocol
                    b'\x00\x00\x00\x01'  # len(members)
                        b'\x00,'  # len(member_id)
                            b'pykafka-d42426fb-c295-4cd9-b585-6dd79daf3afe'  # member_id
                        b'\x00\x07'  # len(client_id)
                            b'pykafka'  # client_id
                        b'\x00\n'  # len(client_host)
                            b'/127.0.0.1'  # client_host
                        b'\x00\x00\x00"'  # len(member_metadata)
                            b'\x00\x00\x00\x00\x00\x01\x00\ndummytopic\x00\x00\x00\x0ctestuserdata'
                        b'\x00\x00\x00H'  # len(member_assignment)
                            b'\x00\x01\x00\x00\x00\x01\x00\x14testtopic_replicated'  # member_assignment
                            b'\x00\x00\x00\n\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00'
                            b'\x02\x00\x00\x00\x08\x00\x00\x00\x03\x00\x00\x00\t'
                            b'\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\x06'
                            b'\x00\x00\x00\x07\x00\x00\x00\x00'
            )
        )
        self.assertTrue(b'testgroup' in response.groups)
        group_response = response.groups[b'testgroup']
        self.assertEqual(group_response.error_code, 0)
        self.assertEqual(group_response.group_id, b'testgroup')
        self.assertEqual(group_response.state, b'Stable')
        self.assertEqual(group_response.protocol_type, b'consumer')
        self.assertEqual(group_response.protocol, b'range')
        member_id = b'pykafka-d42426fb-c295-4cd9-b585-6dd79daf3afe'
        self.assertTrue(member_id in group_response.members)
        member = group_response.members[member_id]
        self.assertEqual(member.member_id, member_id)
        self.assertEqual(member.client_id, b'pykafka')
        self.assertEqual(member.client_host, b'/127.0.0.1')
        metadata = member.member_metadata
        self.assertEqual(metadata.version, 0)
        self.assertEqual(metadata.topic_names, [b"dummytopic"])
        self.assertEqual(metadata.user_data, b"testuserdata")
        assignment = member.member_assignment
        self.assertEqual(assignment.version, 1)
        self.assertEqual(assignment.partition_assignment,
                         [(b'testtopic_replicated', [0, 1, 2, 8, 3, 9, 4, 5, 6, 7])])

    def test_create_topics_request(self):
        req = protocol.CreateTopicsRequest([protocol.CreateTopicRequest(b'mycooltopic', 4,
                                                                        2, [], [])])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x004\x00\x13\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\x00\x00\x01'  # len(topic_reqs)
                    b'\x00\x0b'  # len(topic)  # noqa
                        b'mycooltopic'  # topic
                    b'\x00\x00\x00\x04'  # num_partitions
                    b'\x00\x02'  # replication_factor
                    b'\x00\x00\x00\x00'  # len(replica_assignment)
                    b'\x00\x00\x00\x00'  # len(config_entries)
                b'\x00\x00\x00\x00'  # timeout
            )
        )

    def test_create_topics_response(self):
        response = protocol.CreateTopicsResponse(  # noqa
            bytearray(
                b'\x00\x00\x00\x01'  # len(topic_errors)
                    b'\x00\t'  # len(topic) # noqa
                        b'testtopic'  # topic_name
                    b'\x00\x00'  # error_code
            )
        )

    def test_delete_topics_request(self):
        req = protocol.DeleteTopicsRequest([b'mycooltopic'])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00&\x00\x14\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\x00\x00\x01'  # len(topic_error_codes)  # noqa
                    b'\x00\x0b'  # len(topic)
                        b'mycooltopic'  # topic
                b'\x00\x00\x00\x00'  # timeout
            )
        )

    def test_delete_topics_response(self):
        response = protocol.DeleteTopicsResponse(  # noqa
            bytearray(
                b'\x00\x00\x00\x01'  # len(topic_errors)
                    b'\x00\t'  # len(topic) # noqa
                        b'testtopic'  # topic_name
                    b'\x00\x00'  # error_code
            )
        )

    def test_api_versions_request(self):
        req = protocol.ApiVersionsRequest()
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00\x11\x00\x12\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
            )
        )

    def test_api_versions_response(self):
        response = protocol.ApiVersionsResponse(
            bytearray(
                b'\x00\x00'  # error_code
                b'\x00\x00\x00\x15'  # len(api_versions)
                    b'\x00\x00\x00\x00\x00\x02'  # api_key, min_version, max_version # noqa
                    b'\x00\x01\x00\x00\x00\x03'
                    b'\x00\x02\x00\x00\x00\x01'
                    b'\x00\x03\x00\x00\x00\x02'
                    b'\x00\x04\x00\x00\x00\x00'
                    b'\x00\x05\x00\x00\x00\x00'
                    b'\x00\x06\x00\x00\x00\x02'
                    b'\x00\x07\x00\x01\x00\x01'
                    b'\x00\x08\x00\x00\x00\x02'
                    b'\x00\t\x00\x00\x00\x01'
                    b'\x00\n\x00\x00\x00\x00'
                    b'\x00\x0b\x00\x00\x00\x01'
                    b'\x00\x0c\x00\x00\x00\x00'
                    b'\x00\r\x00\x00\x00\x00'
                    b'\x00\x0e\x00\x00\x00\x00'
                    b'\x00\x0f\x00\x00\x00\x00'
                    b'\x00\x10\x00\x00\x00\x00'
                    b'\x00\x11\x00\x00\x00\x00'
                    b'\x00\x12\x00\x00\x00\x00'
                    b'\x00\x13\x00\x00\x00\x00'
                    b'\x00\x14\x00\x00\x00\x00'
                    b'\x00\x00\x00\x00'  # ???
            )
        )
        self.assertEqual(len(response.api_versions), 21)
        self.assertEqual(response.api_versions[1].max, 3)
        self.assertEqual(response.api_versions[5].min, 0)
        self.assertEqual(response.api_versions[12].key, 12)


if __name__ == '__main__':
    unittest2.main()
