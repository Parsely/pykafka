.. image:: https://travis-ci.org/Parsely/pykafka.svg?branch=master
    :target: https://travis-ci.org/Parsely/pykafka
.. image:: https://codecov.io/github/Parsely/pykafka/coverage.svg?branch=master
    :target: https://codecov.io/github/Parsely/pykafka?branch=master

PyKafka
=======

.. image:: http://i.imgur.com/ztYl4lG.jpg

PyKafka is a cluster-aware Kafka 0.8.2 protocol client for python. It includes python
implementations of Kafka producers and consumers, and runs under python 2.7.

PyKafka's primary goal is to provide a similar level of abstraction to the
`JVM Kafka client`_ using idioms familiar to python programmers and exposing
the most pythonic API possible.

You can install PyKafka from PyPI with

::

    $ pip install pykafka

Full documentation and usage examples for PyKafka can be found on `readthedocs`_.

You can install PyKafka for local development and testing with

    $ python setup.py develop

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

    >>> with topic.get_sync_producer() as producer:
    ...     for i in range(4):
    ...         producer.produce('test message ' + i ** 2)

The example above would produce to kafka synchronously, that is, the call only
returns after we have confirmation that the message made it to the cluster.

To achieve higher throughput however, we recommend using the ``Producer`` in
asynchronous mode.  In that configuration, ``produce()`` calls will return a
``concurrent.futures.Future`` (`docs`_), which you may evaluate later (or, if
reliable delivery is not a concern, you're free to discard it unevaluated).
Here's a rough usage example:

.. sourcecode:: python

    >>> with topic.get_producer() as producer:
    ...     count = 0
    ...     pending = []
    ...     while True:
    ...         count += 1
    ...         future = producer.produce('test message',
    ...                                   partition_key='{}'.format(count))
    ...         pending.append(future)
    ...         if count % 10**5 == 0:  # adjust this or bring lots of RAM ;)
    ...             done, not_done = concurrent.futures.wait(pending,
                                                             timeout=.001)
    ...             for future in done:
    ...                 message_key = future.kafka_msg.partition_key
    ...                 if future.exception() is not None:
    ...                     print 'Failed to deliver message {}: {}'.format(
    ...                         message_key, repr(future.exception()))
    ...                 else:
    ...                     print 'Successfully delivered message {}'.format(
    ...                         message_key)
    ...             pending = list(not_done)

.. _docs: https://pythonhosted.org/futures/#future-objects

You can also consume messages from this topic using a `Consumer` instance.

.. sourcecode:: python

    >>> consumer = topic.get_simple_consumer()
    >>> for message in consumer:
    ...     if message is not None:
    ...         print message.offset, message.value
    0 test message 0
    1 test message 1
    2 test message 4
    3 test message 9

This `SimpleConsumer` doesn't scale - if you have two `SimpleConsumers`
consuming the same topic, they will receive duplicate messages. To get around
this, you can use the `BalancedConsumer`.

.. sourcecode:: python

    >>> balanced_consumer = topic.get_balanced_consumer(
    ...     consumer_group='testgroup',
    ...     auto_commit_enable=True,
    ...     zookeeper_connect='myZkClusterNode1.com:2181,myZkClusterNode2.com:2181/myZkChroot'
    ... )

You can have as many `BalancedConsumer` instances consuming a topic as that
topic has partitions. If they are all connected to the same zookeeper instance,
they will communicate with it to automatically balance the partitions between
themselves.

Operational Tools
-----------------

PyKafka includes a small collection of `CLI tools`_ that can help with common tasks
related to the administration of a Kafka cluster, including offset and lag monitoring and
topic inspection. The full, up-to-date interface for these tools can be fould by running

.. sourcecode::

    $ python cli/kafka_tools.py --help

or after installing PyKafka via setuptools or pip:

.. sourcecode::

    $ kafka-tools --help

.. _CLI tools: https://github.com/Parsely/pykafka/blob/master/pykafka/cli/kafka_tools.py

What happened to Samsa?
-----------------------

This project used to be called samsa. It has been renamed PyKafka and has been
fully overhauled to support Kafka 0.8.2. We chose to target 0.8.2 because the offset
Commit/Fetch API is stabilized.

The Samsa `PyPI package`_  will stay up for the foreseeable future and tags for
previous versions will always be available in this repo.

.. _PyPI package: https://pypi.python.org/pypi/samsa/0.3.11

pykafka or kafka-python?
------------------------

These are two different projects.
See `the discussion here <https://github.com/Parsely/pykafka/issues/334>`_.

Support
-------

If you need help using PyKafka or have found a bug, please open a `github issue`_ or use the `Google Group`_.

.. _github issue: https://github.com/Parsely/pykafka/issues
.. _Google Group: https://groups.google.com/forum/#!forum/pykafka-user
