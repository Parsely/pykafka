import unittest2

from pykafka.utils import compression


class CompressionTests(unittest2.TestCase):
    """Keeping these simple by verifying what goes in is what comes out."""
    text = b"The man in black fled across the desert, and the gunslinger followed."

    def test_gzip(self):
        encoded = compression.encode_gzip(self.text)
        self.assertNotEqual(self.text, encoded)

        decoded = compression.decode_gzip(encoded)
        self.assertEqual(self.text, decoded)

    def test_snappy(self):
        encoded = compression.encode_snappy(self.text)
        self.assertNotEqual(self.text, encoded)

        decoded = compression.decode_snappy(encoded)
        self.assertEqual(self.text, decoded)

    def test_snappy_xerial(self):
        encoded = compression.encode_snappy(self.text, xerial_compatible=True)
        self.assertNotEqual(self.text, encoded)

        decoded = compression.decode_snappy(encoded)
        self.assertEqual(self.text, decoded)


if __name__ == '__main__':
    unittest2.main()
