#####
samsa
#####

|travis-ci|_

.. |travis-ci| image:: http://travis-ci.org/disqus/samsa.png?branch=master
.. _travis-ci: http://travis-ci.org/#!/disqus/samsa

    One morning, as Gregor Samsa was waking up from anxious dreams, he
    discovered that in bed he had been changed into a monstrous verminous bug.

Samsa is a cluster-aware Kafka protocol client and routing library for Python.

It's currently under development, but is being used internally to some success.

************
Introduction
************

What is Kafka?
==============

Kafka is a distributed publish-subscribe messaging system. Cluster management
happens with ZooKeeper.

Messages
    Messages are units of data to be communicated between clients or services.
    Messages don't have any have any inherent structure -- as far as Kafka is
    concerned, the message is just an array of bytes and the application can
    serialize/deserialize the payload in a way that makes sense to it's own environment.
Brokers
    Brokers are nodes/servers that store and serve messages.
Topics
    Messages are published to topics, which are similar to channels. Every topic
    is available on all servers.
Partitions
    Topics are divided into partitions, which are distributed across brokers.
    Each partition is owned by a single broker.

The clients of a Kafka cluster are generally split into two different categories,
although these roles are not mutually exclusive:

Producer
    Producers publish messages to topics.
Consumer
    Consumers subscribe to messages from topics.

Goals of this Project
=====================

* Your application code should not be dependent on broker state or Kafka cluster
  configuration. The only service that it should know about directly is the
  ZooKeeper cluster.
* Provide a level of abstraction and functionality similar to that of the Scala
  client, but using Pythonic idioms and native data structures.

Guarantees
==========

* Every publish will be written to (at least) one node, as long as a node is up
  to accept the write.

**********************
Usage and API Overview
**********************

Creating a Cluster
==================

::

    >>> from kazoo.client import KazooClient
    >>> from samsa.cluster import Cluster
    >>> zookeeper = KazooClient()
    >>> zookeeper.connect()
    >>> cluster = Cluster(zookeeper)

Brokers
=======

To list all of the brokers by their broker ID within the cluster:

::

    >>> print cluster.brokers.keys()
    [0, 1]

To get a ``Broker`` object by ID, provide the ID as the key in a dictionary
item lookup::

    >>> print cluster.brokers[0]
    <samsa.brokers.Broker at 0x1005f4c10: id=0>

Topics
======

``Cluster`` objects also provide a dictionary-like interface for retrieving
``Topic`` objects by name. For example::

    >>> topic = cluster.topics['example-topic']
    >>> print topic
    <samsa.topics.Topic at 0x1005f4d90: name='example-topic'>

You can also see how many partitions are available to accept writes within a
topic by coercing it's ``PartitionMap`` to a list::

    >>> print list(topic.partitions)
    [<samsa.partitions.Partition at 0x1005f9b90: topic=<samsa.topics.Topic at 0x1005f4d90: name='example-topic'>, broker=<samsa.brokers.Broker at 0x1005f4c10: id=0>, number=0>]

Publishing
==========

To publish to a topic, provide a string or list of strings to be published to
a ``Topic`` instances's ``publish`` method::

    >>> topic.publish('hello world')
    >>> topic.publish(['hello', 'world'])

If a list of messages is provided, all messages will be delivered to the same
partition, in order, on the same broker. Individual messages hold no such
guarantee, and are subject to end up on any number of hosts as determined by
the partitioning method, and can be distributed across a number of partitions,
potentially losing any ordering between messages.

Consuming
=========

Consumers are organized into consumer groups, which allows Kafka to provide two
semantically different methods of message consumption, based on the consumer
group configuration.

Queue
    Each consumer in a consumer group receives a message once, which
    provides for a reasonably even distribution of messages around the group
    since it is the owner of only a subset of the available partitions within
    the topic. This is similar to a put/get queue or many AMQP-like interfaces.
Topic
    Each consumer is it's own consumer group, and receives all messages
    sent to the topic since it is the owner of all partitions. This is similar
    to an evented system, or a publish/subscribe interface.

A Note about Partition Counts
-----------------------------

An important thing to note when configuring your broker is that if there are
more consumers than partitions, some consumers won't get any messages at all,
so you should configure your brokers up front to split topics into at least as
many partitions as you expect to have consumers.

To get the minimum number of partitions you should make available, use the
following equation::

    # of brokers * # of partitions per node = # of consumers in the largest consumer group for this topic

Creating Consumers
------------------

To subscribe to a topic, provide a group name to the ``subscribe`` method on a
``Topic`` instance::

    >>> consumer = topic.subscribe('group-name')

Consumers have to be registered with ZooKeeper before they begin to receive
messages. (Remember, only one consumer in a consumer group can be reading from
a partition at any time.)

To listen for messages on an established consumer::

    >>> for message in consumer:
    ...     print message
