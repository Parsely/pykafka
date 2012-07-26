#!/usr/bin/env python
import code
import logging

from kazoo.client import KazooClient

from samsa.cluster import Cluster

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

zookeeper = KazooClient()
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
