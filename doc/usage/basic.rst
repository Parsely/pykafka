Basic Usage
===========

Creating a Cluster
------------------

::

    from kazoo.client import KazooClient
    from samsa.cluster import Cluster

    zookeeper = KazooClient()
    cluster = Cluster(zookeeper)

Publishing to a Topic
---------------------

::

    topic = cluster.topics['topic-name']
    topic.publish('This is a message.')

You can also pass a sequence of messages to ``publish``::

    topic.publish(['This is a message.', 'This is another message.'])

When publishing a sequence of messages, all messages in the sequence are
delivered to the same broker node on the same partition. This allows for strong
causal ordering of messages within the sequence.

Subscribing to a Topic
----------------------

::

    topic = cluster.topics['topic-name']
    consumer = topic.subscribe('my-group-id')
    for message in consumer:
        print message
