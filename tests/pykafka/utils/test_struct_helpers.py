import unittest2

from pykafka.utils import struct_helpers


class StructHelpersTests(unittest2.TestCase):
    def test_basic_unpack(self):
        output = struct_helpers.unpack_from(
            'iiqhi',
            b'\x00\x00\x00\x01\x00\x00\x00\n\x00\x00\x00\x00\x00\x00\x00\n\x00<\x00\x00\x00\x04'
        )
        self.assertEqual(output, (1, 10, 10, 60, 4))

    def test_string_encoding(self):
        output = struct_helpers.unpack_from('S', b'\x00\x04test')
        self.assertEqual(output, (b'test',))

    def test_bytearray_unpacking(self):
        output = struct_helpers.unpack_from('Y', b'\x00\x00\x00\x04test')
        self.assertEqual(output, (b'test',))

    def test_array_unpacking(self):
        output = struct_helpers.unpack_from(
            '[i]',
            b'\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00\x04'
        )
        # A 1-length tuple with a 4-length tuple as the element
        self.assertEqual(output, [1, 2, 3, 4])

    def test_varint_encode(self):
        buff = bytearray(2)
        val = 300
        expected = b'\xac\x02'
        size = struct_helpers.pack_into("V", buff, 0, val)
        self.assertEqual(size, 2)
        self.assertEqual(buff, expected)

    def test_varint_simple(self):
        buff = bytearray(4)
        offset = 0
        val = 69
        fmt = 'V'
        struct_helpers.pack_into(fmt, buff, offset, val)
        output = struct_helpers.unpack_from(fmt, buff)
        self.assertEqual(output, (val,))

    def test_varint_advanced(self):
        buff = bytearray(20)
        offset = 0
        vals = (68, 69, 420, 30001)
        fmt = 'qViV'
        struct_helpers.pack_into(fmt, buff, offset, *vals)
        output = struct_helpers.unpack_from(fmt, buff)
        self.assertEqual(output, vals)


if __name__ == '__main__':
    unittest2.main()
