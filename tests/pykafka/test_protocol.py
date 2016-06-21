import operator
import unittest2

from pykafka import protocol
from pykafka.common import CompressionType
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
                    b'pykafka'  # client id
                # end header

                b'\x00\x00\x00\x00'  # len(topics)
            )
        )

    def test_response(self):
        cluster = protocol.MetadataResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(brokers)
                    b'\x00\x00\x00\x00'  # node id
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
                    b'\x00\x00\x00\x00'  # node is
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
                    b'\x00\x00\x00\x00'  # node id
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


class TestProduceAPI(unittest2.TestCase):
    maxDiff = None

    test_messages = [
        protocol.Message(b'this is a test message', partition_key=b'asdf'),
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
                    b'\x00\x04'  # len(topic name)
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

    def test_gzip_compression(self):
        req = protocol.ProduceRequest(compression_type=CompressionType.GZIP)
        [req.add_message(m, b'test_gzip', 0) for m in self.test_messages]
        msg = req.get_bytes()
        self.assertEqual(len(msg), 207)  # this isn't a good test

    def test_snappy_compression(self):
        req = protocol.ProduceRequest(compression_type=CompressionType.SNAPPY)
        [req.add_message(m, b'test_snappy', 0) for m in self.test_messages]
        msg = req.get_bytes()
        self.assertEqual(len(msg), 212)  # this isn't a good test

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.ProduceResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name)
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
                    b'\x00\x04'  # len(topic name)
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


class TestFetchAPI(unittest2.TestCase):
    maxDiff = None

    expected_data = [
        {
            'partition_key': b'asdf',
            'compression_type': 0,
            'value': b'this is a test message',
            'offset': 0,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'partition': None
        }, {
            'partition_key': b'test_key',
            'compression_type': 0,
            'value': b'this is also a test message',
            'offset': 1,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'partition': None
        }, {
            'partition_key': None,
            'compression_type': 0,
            'value': b"this doesn't have a partition key",
            'offset': 2,
            'partition_id': 0,
            'produce_attempt': 0,
            'delivery_report_q': None,
            'partition': None
        }]

    def msg_to_dict(self, msg):
        """Helper to extract data from Message slots"""
        attr_names = protocol.Message.__slots__
        f = operator.attrgetter(*attr_names)
        return dict(zip(attr_names, f(msg)))

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
                    b'\x00\x04'  # len(topic name)
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
                    b'\x00\x04'  # len(topic name)
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
                    b'\x00\x04'  # len(topic name)
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
            b'\x00\x00\x00\x01'  # len(topics)
            b'\x00\t'  # len(topic name)
                b'test_gzip'  # topic name
            b'\x00\x00\x00\x01'  # len(partitions)
                b'\x00\x00\x00\x00'  # partition
                    b'\x00\x00'  # error code
                    b'\x00\x00\x00\x00\x00\x00\x00\x03'  # highwater mark offset
                    b'\x00\x00\x00\x9b'  # message set size
                        b'\x00\x00\x00\x00\x00\x00\x00\x02'  # offset
                        b'\x00\x00\x00\x8f'  # message size
                            b'\xbb\xe7\x1f\xb8'  # crc
                            b'\x00'  # magic byte
                            b'\x01'  # attributes
                            b'\xff\xff\xff\xff'  # len(key)
                            b'\x00\x00\x00\x81'  # len(value)
                                b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00c`\x80\x03\r\xbe.I\x7f0\x8b%\xb18%\rH\x8b\x95dd\x16+\x00Q\xa2BIjq\x89Bnjqqbz*T=#\x10\x1b\xb2\xf3\xcb\xf4\x81y\x1c \x15\xf1\xd9\xa9\x95@\xb64\\_Nq>v\xcdL@\xac\x7f\xb5(\xd9\x98\x81\xe1?\x10\x00y\x8a`M)\xf9\xa9\xc5y\xea%\n\x19\x89e\xa9@\x9d\x05\x89E%\x99%\x99\xf9y\n@\x93\x01N1\x9f[\xac\x00\x00\x00'  # value
        ])
        response = protocol.FetchResponse(msg)
        for i in range(len(self.expected_data)):
            self.assertDictEqual(
                self.msg_to_dict(response.topics[b'test_gzip'][0].messages[i]),
                self.expected_data[i])

    def test_snappy_decompression(self):
        msg = b''.join([
            b'\x00\x00\x00\x01'  # len(topics)
            b'\x00\x0b'  # len(topic name)
                b'test_snappy'  # topic name
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
        for i in range(len(self.expected_data)):
            self.assertDictEqual(
                self.msg_to_dict(response.topics[b'test_snappy'][0].messages[i]),
                self.expected_data[i])


class TestOffsetAPI(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        preq = protocol.PartitionOffsetRequest(b'test', 0, -1, 1)
        req = protocol.OffsetRequest(partition_requests=[preq, ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x003\x00\x02\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\xff\xff\xff\xff'  # replica id
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
                        b'\xff\xff\xff\xff\xff\xff\xff\xff'  # time
                        b'\x00\x00\x00\x01'  # max number of offsets
            )
        )

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.OffsetResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name)
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
        resp = protocol.OffsetResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x04'  # len(topic name)
                        b'test'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partitoin
                        b'\x00\x00'  # error code
                        b'\x00\x00\x00\x01'  # len(offsets)
                            b'\x00\x00\x00\x00\x00\x00\x00\x02'  # offset
            )
        )
        self.assertEqual(resp.topics[b'test'][0].offset, [2])


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
                    b'test'  # group id
            )
        )

    def test_consumer_metadata_response(self):
        response = protocol.GroupCoordinatorResponse(
            buffer(
                b'\x00\x00'  # error code
                b'\x00\x00\x00\x00'  # coordinator id
                b'\x00\r'  # len(coordinator host)
                    b'emmett-debian'  # coordinator host
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
                    b'test'  # consumer group id
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
                    b'\x00\x0c'  # len(topic name)
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
                b'\x00\x00\x00.\x00\t\x00\x01\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\x04'  # len(consumer group)
                    b'test'  # consumer group
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\t'  # len(topic name)
                        b'testtopic'  # topic name
                    b'\x00\x00\x00\x01'  # len(partitions)
                        b'\x00\x00\x00\x00'  # partition
            )
        )

    def test_offset_fetch_response(self):
        response = protocol.OffsetFetchResponse(
            buffer(
                b'\x00\x00\x00\x01'  # len(topics)
                    b'\x00\x0c'  # len(topic name)
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
                    b'\x00\n'  # len(topic name)
                        b'dummytopic'  # topic name
                    b'\x00\x00\x00\x0c'  # len(userdata)
                        b'testuserdata')  # userdata
        )

    def test_join_group_request(self):
        req = protocol.JoinGroupRequest(b'dummygroup', member_id=b'testmember')
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00|\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\n'  # len(groupid)
                    b'dummygroup'  # groupid
                b'\x00\x00u0'  # session timeout
                b'\x00\n'  # len(memberid)
                    b'testmember'  # memberid
                b'\x00\x08'  # len(protocol type)
                    b'consumer'  # protocol type
                b'\x00\x00\x00\x01'  # len(group protocols)
                    b'\x00\x19'  # len(protocol name)
                        b'pykafkaassignmentstrategy'  # protocol name
                    b'\x00\x00\x00"'  # len(protocol metadata)
                        b'\x00\x00\x00\x00\x00\x01\x00\ndummytopic\x00\x00\x00\x0ctestuserdata'  # protocol metadata
            )
        )

    def test_join_group_response(self):
        response = protocol.JoinGroupResponse(
            bytearray(
                b'\x00\x00'  # error code
                b'\x00\x00\x00\x01'  # generation id
                b'\x00\x17'  # len (group protocol)
                    b'dummyassignmentstrategy'  # group protocol
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
        self.assertEqual(response.leader_id,
                         b'pykafka-b2361322-674c-4e26-9194-305962636e57')
        self.assertEqual(response.member_id,
                         b'pykafka-b2361322-674c-4e26-9194-305962636e57')
        self.assertEqual(response.members,
                         {b'pykafka-b2361322-674c-4e26-9194-305962636e57': b'\x00\x00\x00\x00\x00\x01\x00\ndummytopic\x00\x00\x00\x0ctestuserdata'})

    def test_member_assignment_construction(self):
        assignment = protocol.MemberAssignment([(b"mytopic1", [3, 5, 7, 9]),
                                                (b"mytopic2", [2, 4, 6, 8])])
        msg = assignment.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x01'  # version
                b'\x00\x00\x00\x02'  # len(partition assignment)
                    b'\x00\x08'  # len(topic)
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
                protocol.MemberAssignment([(b"mytopic1", [3, 5, 7, 9]),
                                           (b"mytopic2", [3, 5, 7, 9])], member_id=b"a"),
                protocol.MemberAssignment([(b"mytopic1", [2, 4, 6, 8]),
                                           (b"mytopic2", [2, 4, 6, 8])], member_id=b"b")
            ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(
                b'\x00\x00\x00\xc4\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x07pykafka'  # header
                b'\x00\n'  # len(group id)
                    b'dummygroup'  # group id
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
                    b'\x00\x01\x00\x00\x00\x01\x00\x14testtopic_replicated\x00\x00\x00\n\x00\x00\x00\x06\x00\x00\x00\x07\x00\x00\x00\x08\x00\x00\x00\t\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00\x04\x00\x00\x00\x05,pyk'  # member assignment
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
                    b'dummygroup'  # group id
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
                    b'dummygroup'  # group id
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


if __name__ == '__main__':
    unittest2.main()
