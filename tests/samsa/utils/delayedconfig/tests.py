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

import mock
import unittest2

from samsa.utils.delayedconfig import (DelayedConfiguration,
    requires_configuration)


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
