.. image:: https://travis-ci.org/Parsely/pykafka.svg?branch=master
    :target: https://travis-ci.org/Parsely/pykafka
.. image:: https://coveralls.io/repos/Parsely/pykafka/badge.svg?branch=master
    :target: https://coveralls.io/r/Parsely/pykafka?branch=master 

PyKafka
=======

.. image:: http://i.imgur.com/ztYl4lG.jpg

PyKafka is a cluster-aware Kafka protocol client for python. It includes python
implementations of Kafka producers and consumers.

PyKafka's primary goal is to provide a similar level of abstraction to the
`JVM Kafka client`_ using idioms familiar to python programmers and exposing
the most pythonic API possible.

You can install PyKafka from PyPI with

::

    $ pip install pykafka

Full documentation for PyKafka can be found on `readthedocs`_.

.. _JVM Kafka client: https://github.com/apache/kafka/tree/0.8.2/clients/src/main/java/org/apache/kafka
.. _readthedocs: http://pykafka.readthedocs.org/en/latest/

Getting Started
---------------

Assuming you have a Kafka instance running on localhost, you can use PyKafka
to connect to it.

.. sourcecode:: python

    >>> from pykafka import KafkaClient
    >>> client = KafkaClient(hosts="127.0.0.1:9092")

If the cluster you've connected to has any topics defined on it, you can list
them with:

.. sourcecode:: python

    >>> client.topics
    {'my.test': <pykafka.topic.Topic at 0x19bc8c0 (name=my.test)>}
    >>> topic = client.topics['my.test']

Once you've got a `Topic`, you can create a `Producer` for it and start
producing messages.

.. sourcecode:: python

    >>> producer = topic.get_producer()
    >>> producer.produce(['test message ' + i ** 2 for i in range(4)])

You can also consume messages from this topic using a `Consumer` instance.

.. sourcecode:: python

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

.. sourcecode:: python

    >>> balanced_consumer = topic.get_balanced_consumer(
        consumer_group='testgroup',
        auto_commit_enable=True,
        zookeeper_connect='myZkClusterNode1.com:2181,myZkClusterNode2.com:2181/myZkChroot'
    )

You can have as many `BalancedConsumer` instances consuming a topic as that
topic has partitions. If they are all connected to the same zookeeper instance,
they will communicate with it to automatically balance the partitions between
themselves.

What happened to Samsa?
-----------------------

This project used to be called samsa. It has been renamed PyKafka and has been
fully overhauled to support Kafka 0.8.2. We chose to target 0.8.2 because it's
currently the latest stable version, and the Offset Commit/Fetch API is
stabilized.

The Samsa `PyPI package`_  will stay up for the foreseeable future and tags for
previous versions will always be available in this repo.

.. _PyPI package: https://pypi.python.org/pypi/samsa/0.3.11

Support
-------

If you need help using PyKafka or have found a bug, please open a `github issue`_ or use the `Google Group`_.

.. _github issue: https://github.com/Parsely/pykafka/issues
.. _Google Group: https://groups.google.com/forum/#!forum/pykafka-user
