__license__ = """
Copyright 2012 DISQUS

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

import unittest2

from samsa.utils.structuredio import StructuredBytesIO


class StructuredBytesIOTest(unittest2.TestCase):
    def test_length(self):
        buf = StructuredBytesIO('\x00\x00')
        self.assertEqual(len(buf), 2)

    def test_string_cast(self):
        value = '\x00\x00'
        buf = StructuredBytesIO(value)
        self.assertEqual(str(buf), value)
        buf.seek(2)  # SEEK_END
        self.assertEqual(str(buf), value)

    def test_write(self):
        buf = StructuredBytesIO()
        buf.write('test')
        self.assertEqual(str(buf), 'test')

        buf = StructuredBytesIO()
        buf.write(StructuredBytesIO('test'))
        self.assertEqual(str(buf), 'test')

    def test_pack(self):
        buf = StructuredBytesIO()
        buf.pack(2, 1)
        self.assertEqual(str(buf), '\x00\x01')

    def test_unpack(self):
        buf = StructuredBytesIO('\x00\x01')
        value = buf.unpack(2)
        self.assertEqual(value, 1)

    def test_frame(self):
        buf = StructuredBytesIO()
        buf.frame(1, 'test')
        self.assertEqual(str(buf), '\x04test')

    def test_unframe(self):
        buf = StructuredBytesIO('\x04test')
        self.assertEqual(buf.unframe(1), 'test')

        buf = StructuredBytesIO('\x04testextra')
        self.assertEqual(buf.unframe(1), 'test')

        buf = StructuredBytesIO('\x04tes')
        with self.assertRaises(ValueError):
            buf.unframe(1)

        buf = StructuredBytesIO('\x04tes')
        self.assertEqual(buf.unframe(1, validate=False), 'tes')

    def test_wrap(self):
        buf = StructuredBytesIO('test').wrap(1)
        self.assertIsInstance(buf, StructuredBytesIO)
        self.assertEqual(str(buf), '\x04test')

    def test_unwrap(self):
        buf = StructuredBytesIO('\x04test').unwrap(1)
        self.assertIsInstance(buf, StructuredBytesIO)
        self.assertEqual(str(buf), 'test')
