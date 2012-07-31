from unittest import TestCase

from samsa.config import ConsumerConfig


class Configured(object):

    def __init__(self, **kwargs):
        self.config = ConsumerConfig.build(kwargs)


class TestConfig(TestCase):

    def test_default(self):
        obj = Configured()

        self.assertEquals(obj.config['socket_buffersize'], ConsumerConfig.socket_buffersize)

    def test_override(self):

        obj = Configured(autocommit_enable=False)
        self.assertFalse(obj.config['autocommit_enable'])

    def test_raises(self):

        self.assertRaises(AttributeError, Configured, foo='bar')
