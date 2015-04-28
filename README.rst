.. image:: https://travis-ci.org/Parsely/pykafka.svg?branch=master

PyKafka
=======

.. image:: http://i.imgur.com/ztYl4lG.jpg

PyKafka is a cluster-aware Kafka protocol client for python. It includes python
implementations of Kafka producers and consumers.

PyKafka's primary goal is to provide a similar level of abstraction to the
`JVM Kafka client`_ using idioms familiar to python programmers and exposing
the most pythonic API possible.

.. _JVM Kafka client: https://github.com/apache/kafka/tree/0.8.2/clients/src/main/java/org/apache/kafka

What happened to Samsa?
-----------------------

This project used to be called samsa. It has been renamed PyKafka and has been
fully overhauled to support Kafka 0.8.2. We chose to target 0.8.2 because it's
currently the latest stable version, and the Offset Commit/Fetch API is
stabilized.

The Samsa `PyPI package`_  will stay up for the foreseeable future and tags for
previous versions will always be available in this repo.

.. _PyPI package: https://pypi.python.org/pypi/samsa/0.3.11

Documentation
-------------

Documentation for PyKafka can be found on `readthedocs`_.

.. _readthedocs: http://pykafka.readthedocs.org/en/latest/

Kafka
-----

`Apache Kafka`_ is a distributed log-based messaging system. It provides an
abstraction that allows *producers* to send messages to a cluster and for
*consumers* to fetch messages from that cluster. The *consumer group* mechanism
allows the semantics to resemble either publish-subscribe or message queueing.
Kafka uses `Apache ZooKeeper`_ to facilitate information sharing between its
servers.

Messages
    Messages are units of data to be communicated between clients.
    Messages don't have any inherent structure -- as far as Kafka is
    concerned, the message is just an array of bytes and the application can
    serialize or deserialize the payload in a way that makes sense to its own
    environment.
Brokers
    Brokers are servers that store and serve messages.
Topics
    The topic is Kafka's concept of a single abstracted log.
    Messages are published to topics. Every topic is available on all servers.
Partitions
    Topics are divided into partitions, which are distributed across brokers.
    Each partition is owned by a single broker.

The clients of a Kafka cluster are generally split into two different categories,
although these roles are not mutually exclusive:

Producer
    Producers publish messages to topics.
Consumer
    Consumers consume messages from topics.

For more information about Kafka itself, visit the `Kafka documentation`_.

.. _Apache Kafka: http://kafka.apache.org/documentation.html
.. _Apache ZooKeeper: https://zookeeper.apache.org/
.. _Kafka Documentation: http://kafka.apache.org/documentation.html

Usage and API Overview
----------------------

Assuming you have a Kafka instance running on localhost, you can use PyKafka
to connect to it.

::

    >>> from pykafka import KafkaClient
    >>> client = KafkaClient(hosts="127.0.0.1:9092")

If the cluster you've connected to has any topics defined on it, you can list
them with:

::

    >>> topic = client.topics['my.test']

Once you've got a `Topic`, you can create a `Producer` for it and start
producing messages.

::

    >>> producer = topic.get_producer()
    >>> producer.produce(['test message ' + i ** 2 for i in range(4)])

You can also consume messages from this topic using a `Consumer` instance.

::

    >>> consumer = topic.get_simple_consumer()
    >>> for message in consumer:
        if message is not None:
            print message.offset, message.value
    0 test message 0
    1 test message 1
    2 test message 4
    3 test message 9

This `SimpleConsumer` doesn't scale - if you have two `SimpleConsumers`
consuming the same topic, they will receive duplicate messages. To get around
this, you can use the `BalancedConsumer`.

::

    >>> balanced_consumer = topic.get_balanced_consumer(
            consumer_group='testgroup', auto_commit_enable=True)

You can have as many `BalancedConsumer` instances consuming a topic as that
topic has partitions. If they are all connected to the same zookeeper instance,
they will communicate with it to automatically balance the partitions between
themselves.

Support
-------

If you need help using PyKafka or have found a bug, please open a `github issue`_.

.. _github issue: https://github.com/Parsely/pykafka/issues
