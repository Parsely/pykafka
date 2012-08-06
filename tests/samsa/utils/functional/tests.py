import itertools
import unittest2

from samsa.utils.functional import methodmap, methodimap


class MethodMapTestCase(unittest2.TestCase):
    def setUp(self):
        self.values = ['hello  ', ' world ']
        self.expected = ['hello', 'world']
        self.method = 'strip'

    def test_methodmap(self):
        self.assertEqual(methodmap(self.method, self.values), self.expected)

    def test_methodimap(self):
        result = methodimap(self.method, self.values)
        self.assertIsInstance(result, itertools.imap)
        self.assertEqual(list(result), expected)
