import mock
import unittest2

from samsa.utils.delayedconfig import DelayedConfiguration, requires_configuration


class Example(DelayedConfiguration):
    _configure = mock.Mock()

    @requires_configuration
    def method(self, *args, **kwargs):
        pass


class DelayedConfigurationTests(unittest2.TestCase):
    def test_delayed_configuration(self):
        instance = Example()
        self.assertEqual(instance._configure.call_count, 0)

        instance.method()
        self.assertEqual(instance._configure.call_count, 1)

        instance.method()
        self.assertEqual(instance._configure.call_count, 1)
