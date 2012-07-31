from unittest import TestCase

from samsa.config import ConsumerConfig


class Configured(object):

    def __init__(self, **kwargs):
        self.config = ConsumerConfig.init(kwargs)


class TestConfig(TestCase):

    def test_default(self):
        obj = Configured()

        self.assertEquals(obj.config['socket_buffersize'], ConsumerConfig.socket_buffersize)

    def test_override(self):

        obj = Configured(autocommit_interval_ms=False)
        self.assertFalse(obj.config['autocommit_interval_ms'])
