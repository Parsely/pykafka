import mock
import unittest2
from nose.plugins.attrib import attr


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


@attr('integration')
class ClientIntegrationTestCase(unittest2.TestCase):
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
