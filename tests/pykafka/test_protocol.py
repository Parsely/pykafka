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
            bytearray(b'\x00\x00\x00\x15\x00\x03\x00\x00\x00\x00\x00\x00\x00\x07pykafka\x00\x00\x00\x00')
        )

    def test_response(self):
        cluster = protocol.MetadataResponse(
            buffer(b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x09localhost\x00\x00#\x84\x00\x00\x00\x01\x00\x00\x00\x04test\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00')
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
            buffer(b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x09localhost\x00\x00#\x84\x00\x00\x00\x01\x00\x00\x00\x04test\x00\x00\x00\x02\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00')
        )
        self.assertEqual(response.topics[b'test'].partitions[0].err, 3)

    def test_topic_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.MetadataResponse(
                buffer(b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x09localhost\x00\x00#\x84\x00\x00\x00\x01\x00\x03\x00\x04test\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00')
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
            bytearray(b"\x00\x00\x00a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07pykafka\x00\x01\x00\x00\'\x10\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x004\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00(\x0e\x8a\x19O\x00\x00\x00\x00\x00\x04asdf\x00\x00\x00\x16this is a test message")
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
                buffer(b'\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x02')
        )
        self.assertEqual(response.topics[b'test'][0].err, 3)

    def test_response(self):
        response = protocol.ProduceResponse(
            buffer(b'\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02')
        )
        self.assertEqual(
            response.topics,
            {b'test': {0: protocol.ProducePartitionResponse(0, 2)}}
        )


class TestFetchAPI(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        preq = protocol.PartitionFetchRequest(b'test', 0, 1)
        req = protocol.FetchRequest(partition_requests=[preq, ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x00;\x00\x01\x00\x00\x00\x00\x00\x00\x00\x07pykafka\xff\xff\xff\xff\x00\x00\x03\xe8\x00\x00\x04\x00\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x10\x00\x00')
        )

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.FetchResponse(
            buffer(b'\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00B\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x006\xa3 ^B\x00\x00\x00\x00\x00\x12test_partition_key\x00\x00\x00\x16this is a test message')
        )
        self.assertEqual(response.topics[b'test'][0].err, 3)

    def test_response(self):
        resp = protocol.FetchResponse(
            buffer(b'\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00B\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x006\xa3 ^B\x00\x00\x00\x00\x00\x12test_partition_key\x00\x00\x00\x16this is a test message')
        )
        self.assertEqual(len(resp.topics[b'test'][0].messages), 1)
        self.assertEqual(resp.topics[b'test'][0].max_offset, 2)
        message = resp.topics[b'test'][0].messages[0]
        self.assertEqual(message.value, b'this is a test message')
        self.assertEqual(message.partition_key, b'test_partition_key')
        self.assertEqual(message.compression_type, 0)
        self.assertEqual(message.offset, 1)

    def test_gzip_decompression(self):
        msg = b'\x00\x00\x00\x01\x00\ttest_gzip\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x9b\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x8f\xbb\xe7\x1f\xb8\x00\x01\xff\xff\xff\xff\x00\x00\x00\x81\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00c`\x80\x03\r\xbe.I\x7f0\x8b%\xb18%\rH\x8b\x95dd\x16+\x00Q\xa2BIjq\x89Bnjqqbz*T=#\x10\x1b\xb2\xf3\xcb\xf4\x81y\x1c \x15\xf1\xd9\xa9\x95@\xb64\\_Nq>v\xcdL@\xac\x7f\xb5(\xd9\x98\x81\xe1?\x10\x00y\x8a`M)\xf9\xa9\xc5y\xea%\n\x19\x89e\xa9@\x9d\x05\x89E%\x99%\x99\xf9y\n@\x93\x01N1\x9f[\xac\x00\x00\x00'
        response = protocol.FetchResponse(msg)
        expected1 = {
            'partition_key': b'asdf',
            'compression_type': 0,
            'value': b'this is a test message',
            'offset': 0,
            'partition_id': 0,
            'produce_attempt': 0,
            'partition': None
        }
        self.assertDictEqual(
            response.topics[b'test_gzip'][0].messages[0].__dict__,
            expected1
        )
        expected2 = {
            'partition_key': b'test_key',
            'compression_type': 0,
            'value': b'this is also a test message',
            'offset': 1,
            'partition_id': 0,
            'produce_attempt': 0,
            'partition': None
        }
        self.assertDictEqual(
            response.topics[b'test_gzip'][0].messages[1].__dict__,
            expected2
        )
        expected3 = {
            'partition_key': None,
            'compression_type': 0,
            'value': b"this doesn't have a partition key",
            'offset': 2,
            'partition_id': 0,
            'produce_attempt': 0,
            'partition': None
        }

        self.assertDictEqual(
            response.topics[b'test_gzip'][0].messages[2].__dict__,
            expected3
        )
        return

    def snappy_decompression(self):
        msg = '\x00\x00\x00\x01\x00\x0btest_snappy\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\xb5\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\xa9\xc1\xf2\xa3\xe1\x00\x02\xff\xff\xff\xff\x00\x00\x00\x9b\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x87\xac\x01\x00\x00\x19\x01\x10(\x0e\x8a\x19O\x05\x0fx\x04asdf\x00\x00\x00\x16this is a test message\x05$(\x00\x00\x01\x00\x00\x001\x07\x0f\x1c\x8e\x05\x10\x00\x08\x01"\x1c_key\x00\x00\x00\x1b\x158\x08lsoV=\x00H\x02\x00\x00\x00/\xd5rc3\x00\x00\xff\xff\xff\xff\x00\x00\x00!\x055ldoesn\'t have a partition key'
        response = protocol.FetchResponse(msg)
        self.assertDictEqual(
            response.topics['test_snappy'][0].messages[0].__dict__,
            {'partition_key': 'asdf', 'compression_type': 0, 'value': 'this is a test message', 'offset': 0},
        )
        self.assertDictEqual(
            response.topics['test_snappy'][0].messages[1].__dict__,
            {'partition_key': 'test_key', 'compression_type': 0, 'value': 'this is also a test message', 'offset': 1},
        )
        self.assertDictEqual(
            response.topics['test_snappy'][0].messages[2].__dict__,
            {'partition_key': None, 'compression_type': 0, 'value': "this doesn't have a partition key", 'offset': 2}
        )


class TestOffsetAPI(unittest2.TestCase):
    maxDiff = None

    def test_request(self):
        preq = protocol.PartitionOffsetRequest(b'test', 0, -1, 1)
        req = protocol.OffsetRequest(partition_requests=[preq, ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x003\x00\x02\x00\x00\x00\x00\x00\x00\x00\x07pykafka\xff\xff\xff\xff\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01')
        )

    def test_partition_error(self):
        # Response has a UnknownTopicOrPartition error for test/0
        response = protocol.OffsetResponse(
            buffer(b'\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02')
        )
        self.assertEqual(response.topics[b'test'][0].err, 3)

    def test_response(self):
        resp = protocol.OffsetResponse(
            buffer(b'\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02')
        )
        self.assertEqual(resp.topics[b'test'][0].offset, [2])


class TestOffsetCommitFetchAPI(unittest2.TestCase):
    maxDiff = None

    def test_consumer_metadata_request(self):
        req = protocol.ConsumerMetadataRequest(b'test')
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x00\x17\x00\n\x00\x00\x00\x00\x00\x00\x00\x07pykafka\x00\x04test')
        )

    def test_consumer_metadata_response(self):
        response = protocol.ConsumerMetadataResponse(
            buffer(b'\x00\x00\x00\x00\x00\x00\x00\remmett-debian\x00\x00#\x84')
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
            bytearray(b'\x00\x00\x00T\x00\x08\x00\x01\x00\x00\x00\x00\x00\x07pykafka\x00\x04test\x00\x00\x00\x01\x00\x07pykafka\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00D\x00\x00\x00\x00U\x08\xad\x82\x00\x0ctestmetadata')
        )

    def test_offset_commit_response(self):
        response = protocol.OffsetCommitResponse(
            buffer(b'\x00\x00\x00\x01\x00\x0cemmett.dummy\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00')
        )
        self.assertEqual(response.topics[b'emmett.dummy'][0].err, 0)

    def test_offset_fetch_request(self):
        preq = protocol.PartitionOffsetFetchRequest(b'testtopic', 0)
        req = protocol.OffsetFetchRequest(b'test', partition_requests=[preq, ])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x00.\x00\t\x00\x01\x00\x00\x00\x00\x00\x07pykafka\x00\x04test\x00\x00\x00\x01\x00\ttesttopic\x00\x00\x00\x01\x00\x00\x00\x00')
        )

    def test_offset_fetch_response(self):
        response = protocol.OffsetFetchResponse(
            buffer(b'\x00\x00\x00\x01\x00\x0cemmett.dummy\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00')
        )
        self.assertEqual(response.topics[b'emmett.dummy'][0].metadata, b'')
        self.assertEqual(response.topics[b'emmett.dummy'][0].offset, 1)


if __name__ == '__main__':
    unittest2.main()
