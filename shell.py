"""
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

#!/usr/bin/env python
import code
import logging
from optparse import OptionParser

from kazoo.client import KazooClient

from samsa.cluster import Cluster

parser = OptionParser()
parser.add_option('--zookeeper', help='zookeeper hosts', default=None)
parser.add_option('--log-level', dest='loglevel', help='log level', default='DEBUG')

options, args = parser.parse_args()

zookeeper_kwargs = {}
if options.zookeeper is not None:
    zookeeper_kwargs['hosts'] = options.zookeeper

logger = logging.getLogger()
logger.setLevel(getattr(logging, options.loglevel.upper()))
logger.addHandler(logging.StreamHandler())

zookeeper = KazooClient(**zookeeper_kwargs)
zookeeper.connect()

kafka = Cluster(zookeeper)

exposed = {
    'kafka': kafka,
    'zookeeper': zookeeper,
}

banner = """---
Welcome to Samsa! We've provided some local variables for you:
%s
---""" % '\n'.join('> %s: %s' % (key, repr(value)) for key, value in exposed.iteritems())

code.interact(banner, local=exposed)
