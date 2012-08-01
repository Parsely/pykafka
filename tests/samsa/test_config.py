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
