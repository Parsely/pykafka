import unittest

from samsa import common, exceptions, protocol
from samsa.utils import compression


class TestMetadataAPI(unittest.TestCase):
    def test_request(self):
        req = protocol.MetadataRequest()
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x00\x13\x00\x03\x00\x00\x00\x00\x00\x00\x00\x05samsa\x00\x00\x00\x00')
        )

    def test_response(self):
        cluster = protocol.MetadataResponse(
            buffer('\x00\x00\x00\x01\x00\x00\x00\x00\x00\x09localhost\x00\x00#\x84\x00\x00\x00\x01\x00\x00\x00\x04test\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00')
        )
        self.assertEqual(cluster.brokers[0].host, 'localhost')
        self.assertEqual(cluster.brokers[0].port, 9092)
        self.assertEqual(cluster.topics['test'].partitions[0].leader,
                         cluster.brokers[0].id)
        self.assertEqual(cluster.topics['test'].partitions[0].replicas,
                         [cluster.brokers[0].id])
        self.assertEqual(cluster.topics['test'].partitions[0].isr,
                         [cluster.brokers[0].id])

    def test_partition_error(self):
        self.assertRaises(
            exceptions.UnknownTopicOrPartition,
            lambda: protocol.MetadataResponse(
                buffer('\x00\x00\x00\x01\x00\x00\x00\x00\x00\x09localhost\x00\x00#\x84\x00\x00\x00\x01\x00\x00\x00\x04test\x00\x00\x00\x02\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00')
            )
        )

    def test_topic_error(self):
        self.assertRaises(
            exceptions.UnknownTopicOrPartition,
            lambda: protocol.MetadataResponse(
                buffer('\x00\x00\x00\x01\x00\x00\x00\x00\x00\x09localhost\x00\x00#\x84\x00\x00\x00\x01\x00\x03\x00\x04test\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00')
            )
        )


class TestProduceAPI(unittest.TestCase):
    test_messages = [
        common.Message('this is a test message', partition_key='asdf'),
        common.Message('this is also a test message', partition_key='test_key'),
        common.Message("this doesn't have a partition key"),
    ]

    def test_request(self):
        message = self.test_messages[0]
        req = protocol.ProduceRequest()
        req.add_messages([message], 'test', 0)
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b"\x00\x00\x00_\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05samsa\x00\x01\x00\x00\'\x10\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x004\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00(\x0e\x8a\x19O\x00\x00\x00\x00\x00\x04asdf\x00\x00\x00\x16this is a test message")
        )

    def test_gzip_compression(self):
        req = protocol.ProduceRequest(compression_type=compression.GZIP)
        req.add_messages(self.test_messages, 'test_gzip', 0)
        msg = req.get_bytes()
        self.assertEqual(len(msg), 205) # this isn't a good test

    def test_snappy_compression(self):
        req = protocol.ProduceRequest(compression_type=compression.SNAPPY)
        req.add_messages(self.test_messages, 'test_snappy', 0)
        msg = req.get_bytes()
        self.assertEqual(len(msg), 210) # this isn't a good test

    def test_partition_error(self):
        self.assertRaises(
            exceptions.UnknownTopicOrPartition,
            lambda: protocol.ProduceResponse(
                buffer('\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x02')
            )
        )

    def test_response(self):
        response = protocol.ProduceResponse(
            buffer('\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02')
        )
        self.assertEqual(response.topics, {'test': {0: 2}})


class TestFetchAPI(unittest.TestCase):
    def test_request(self):
        preq = protocol.PartitionFetchRequest('test', 0, 1)
        req = protocol.FetchRequest(partition_requests=[preq,])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x009\x00\x01\x00\x00\x00\x00\x00\x00\x00\x05samsa\xff\xff\xff\xff\x00\x00\x03\xe8\x00\x00\x04\x00\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x04\xb0\x00')
        )

    def test_partition_error(self):
        self.assertRaises(
            exceptions.UnknownTopicOrPartition,
            lambda: protocol.FetchResponse(
                buffer('\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00B\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x006\xa3 ^B\x00\x00\x00\x00\x00\x12test_partition_key\x00\x00\x00\x16this is a test message')
            )
        )

    def test_response(self):
        resp = protocol.FetchResponse(
            buffer('\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00B\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x006\xa3 ^B\x00\x00\x00\x00\x00\x12test_partition_key\x00\x00\x00\x16this is a test message')
        )
        self.assertEqual(len(resp.topics['test'].messages), 1)
        self.assertEqual(resp.topics['test'].max_offset, 2)
        message = resp.topics['test'].messages[0]
        self.assertEqual(message.value, 'this is a test message')
        self.assertEqual(message.partition_key, 'test_partition_key')
        self.assertEqual(message.compression_type, 0)
        self.assertEqual(message.offset, 1)

    def test_gzip_decompression(self):
        msg = '\x00\x00\x00\x01\x00\ttest_gzip\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x9b\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x8f\xbb\xe7\x1f\xb8\x00\x01\xff\xff\xff\xff\x00\x00\x00\x81\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00c`\x80\x03\r\xbe.I\x7f0\x8b%\xb18%\rH\x8b\x95dd\x16+\x00Q\xa2BIjq\x89Bnjqqbz*T=#\x10\x1b\xb2\xf3\xcb\xf4\x81y\x1c \x15\xf1\xd9\xa9\x95@\xb64\\_Nq>v\xcdL@\xac\x7f\xb5(\xd9\x98\x81\xe1?\x10\x00y\x8a`M)\xf9\xa9\xc5y\xea%\n\x19\x89e\xa9@\x9d\x05\x89E%\x99%\x99\xf9y\n@\x93\x01N1\x9f[\xac\x00\x00\x00'
        response = protocol.FetchResponse(msg)
        self.assertDictEqual(
            response.topics['test_gzip'].messages[0].__dict__,
            {'partition_key': 'asdf', 'compression_type': 0, 'value': 'this is a test message', 'offset': 0},
        )
        self.assertDictEqual(
            response.topics['test_gzip'].messages[1].__dict__,
            {'partition_key': 'test_key', 'compression_type': 0, 'value': 'this is also a test message', 'offset': 1},
        )
        self.assertDictEqual(
            response.topics['test_gzip'].messages[2].__dict__,
            {'partition_key': None, 'compression_type': 0, 'value': "this doesn't have a partition key", 'offset': 2}
        )
        return

    def test_snappy_decompression(self):
        msg = '\x00\x00\x00\x01\x00\x0btest_snappy\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\xb5\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\xa9\xc1\xf2\xa3\xe1\x00\x02\xff\xff\xff\xff\x00\x00\x00\x9b\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x87\xac\x01\x00\x00\x19\x01\x10(\x0e\x8a\x19O\x05\x0fx\x04asdf\x00\x00\x00\x16this is a test message\x05$(\x00\x00\x01\x00\x00\x001\x07\x0f\x1c\x8e\x05\x10\x00\x08\x01"\x1c_key\x00\x00\x00\x1b\x158\x08lsoV=\x00H\x02\x00\x00\x00/\xd5rc3\x00\x00\xff\xff\xff\xff\x00\x00\x00!\x055ldoesn\'t have a partition key'
        response = protocol.FetchResponse(msg)
        self.assertDictEqual(
            response.topics['test_snappy'].messages[0].__dict__,
            {'partition_key': 'asdf', 'compression_type': 0, 'value': 'this is a test message', 'offset': 0},
        )
        self.assertDictEqual(
            response.topics['test_snappy'].messages[1].__dict__,
            {'partition_key': 'test_key', 'compression_type': 0, 'value': 'this is also a test message', 'offset': 1},
        )
        self.assertDictEqual(
            response.topics['test_snappy'].messages[2].__dict__,
            {'partition_key': None, 'compression_type': 0, 'value': "this doesn't have a partition key", 'offset': 2}
        )


class TestOffsetAPI(unittest.TestCase):
    def test_request(self):
        preq = protocol.PartitionOffsetRequest('test', 0, -1, 1)
        req = protocol.OffsetRequest(partition_requests=[preq,])
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x001\x00\x02\x00\x00\x00\x00\x00\x00\x00\x05samsa\xff\xff\xff\xff\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01')
        )

    def test_partition_error(self):
        self.assertRaises(
            exceptions.UnknownTopicOrPartition,
            lambda: protocol.OffsetResponse(
                buffer('\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02')
            )
        )

    def test_response(self):
        resp = protocol.OffsetResponse(
            buffer('\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02')
        )
        self.assertEqual(resp.topics['test'], {0: [2]})


if __name__ == '__main__':
    unittest.main()
