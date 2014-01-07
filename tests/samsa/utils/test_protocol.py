import unittest

from samsa import common
from samsa.utils import protocol

"""
        TODO: Goes in client. Will remove later
        import pdb; pdb.set_trace()
        msg = protocol.MetadataRequest().to_message()
        import socket
        conn = socket.create_connection(('localhost', 9092))
        conn.sendall(msg)
        h = conn.recv(4)
        import struct
        ln = struct.unpack('!i', h)[0]
        resp = conn.recv(ln)
        import pdb; pdb.set_trace()
        resp = protocol.MetadataResponse(resp[4:]).to_cluster()
        from pprint import pprint; pprint(resp)
        import pdb; pdb.set_trace()
    def recvall(socket, length):
        output = bytearray(length)
        received = 0
        while received < length:
            received += socket.recv_into(output, min(length - received, 4096))
        return buffer(output)

"""

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
        ).to_cluster()
        self.assertEqual(cluster.brokers[0].host, 'localhost')
        self.assertEqual(cluster.brokers[0].port, 9092)
        self.assertEqual(cluster.topics['test'].partitions[0].leader,
                         cluster.brokers[0])
        self.assertEqual(cluster.topics['test'].partitions[0].replicas,
                         [cluster.brokers[0]])
        self.assertEqual(cluster.topics['test'].partitions[0].isr,
                         [cluster.brokers[0]])


class TestProduceAPI(unittest.TestCase):
    def test_request(self):
        message = common.Message('this is a test message', partition_key='asdf')
        req = protocol.ProduceRequest()
        req.add_messages([message], 'test', 0)
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x00_\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05samsa\x00\x01\x00\x00\x03\xe8\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x004\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00(\x0e\x8a\x19O\x00\x00\x00\x00\x00\x04asdf\x00\x00\x00\x16this is a test message')
        )

    def test_response(self):
        response = protocol.ProduceResponse(
            buffer('\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02')
        )
        self.assertEqual(response.topics, {'test': {0: 2}})


class TestFetchAPI(unittest.TestCase):
    def test_request(self):
        req = protocol.FetchRequest()
        req.add_fetch('test', 0, 1)
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x009\x00\x01\x00\x00\x00\x00\x00\x00\x00\x05samsa\xff\xff\xff\xff\x00\x00\x03\xe8\x00\x00\x04\x00\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x04\xb0\x00')
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
        self.assertEqual(message.compression, 0)
        self.assertEqual(message.offset, 1)


class TestOffsetAPI(unittest.TestCase):
    def test_request(self):
        req = protocol.OffsetRequest()
        req.add_topic('test', 0, -1)
        msg = req.get_bytes()
        self.assertEqual(
            msg,
            bytearray(b'\x00\x00\x001\x00\x02\x00\x00\x00\x00\x00\x00\x00\x05samsa\xff\xff\xff\xff\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01')
        )

    def test_response(self):
        resp = protocol.OffsetResponse(
            buffer('\x00\x00\x00\x01\x00\x04test\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02')
        )
        self.assertEqual(resp.topics['test'], {0: [2]})


if __name__ == '__main__':
    unittest.main()
