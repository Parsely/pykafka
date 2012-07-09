import mock
import unittest2

from samsa.client import TIME_EARLIEST, TIME_LATEST, Client


class ClientTestCase(unittest2.TestCase):
    def test_produce(self):
        raise NotImplementedError

    def test_multiproduce(self):
        raise NotImplementedError

    def test_fetch(self):
        raise NotImplementedError

    def test_multifetch(self):
        raise NotImplementedError

    def test_offsets(self):
        raise NotImplementedError
