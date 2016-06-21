.. image:: https://travis-ci.org/Parsely/pykafka.svg?branch=master
    :target: https://travis-ci.org/Parsely/pykafka
.. image:: https://codecov.io/github/Parsely/pykafka/coverage.svg?branch=master
    :target: https://codecov.io/github/Parsely/pykafka?branch=master

PyKafka
=======

.. image:: http://i.imgur.com/ztYl4lG.jpg

PyKafka is a cluster-aware Kafka>=0.8.2 client for Python. It includes Python
implementations of Kafka producers and consumers, which are optionally backed
by a C extension built on `librdkafka`_, and runs under Python 2.7+, Python 3.4+,
and PyPy.

.. _librdkafka: https://github.com/edenhill/librdkafka

PyKafka's primary goal is to provide a similar level of abstraction to the
`JVM Kafka client`_ using idioms familiar to Python programmers and exposing
the most Pythonic API possible.

You can install PyKafka from PyPI with

::

    $ pip install pykafka

Full documentation and usage examples for PyKafka can be found on `readthedocs`_.

You can install PyKafka for local development and testing by cloning this repository and
running

::

    $ python setup.py develop

.. _JVM Kafka client: https://github.com/apache/kafka/tree/0.8.2/clients/src/main/java/org/apache/kafka
.. _readthedocs: http://pykafka.readthedocs.org/en/latest/

Getting Started
---------------

Assuming you have at least one Kafka instance running on localhost, you can use PyKafka
to connect to it.

.. sourcecode:: python

    >>> from pykafka import KafkaClient
    >>> client = KafkaClient(hosts="127.0.0.1:9092,127.0.0.1:9093,...")

Or, for a TLS connection, you might write (and also see ``SslConfig`` docs
for further details):

.. sourcecode:: python

    >>> from pykafka import KafkaClient, SslConfig
    >>> config = SslConfig(cafile='/your/ca.cert',
    ...                    certfile='/your/client.cert',  # optional
    ...                    keyfile='/your/client.key',  # optional
    ...                    password='unlock my client key please')  # optional
    >>> client = KafkaClient(hosts="127.0.0.1:<ssl-port>,...",
    ...                      ssl_config=config)

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
    ...         producer.produce('test message ' + str(i ** 2))

The example above would produce to kafka synchronously - the call only
returns after we have confirmation that the message made it to the cluster.

To achieve higher throughput, we recommend using the ``Producer`` in
asynchronous mode, so that ``produce()`` calls will return immediately and the
producer may opt to send messages in larger batches.  You can still obtain
delivery confirmation for messages, through a queue interface which can be
enabled by setting ``delivery_reports=True``.  Here's a rough usage example:

.. sourcecode:: python

    >>> with topic.get_producer(delivery_reports=True) as producer:
    ...     count = 0
    ...     while True:
    ...         count += 1
    ...         producer.produce('test msg', partition_key='{}'.format(count))
    ...         if count % 10 ** 5 == 0:  # adjust this or bring lots of RAM ;)
    ...             while True:
    ...                 try:
    ...                     msg, exc = producer.get_delivery_report(block=False)
    ...                     if exc is not None:
    ...                         print 'Failed to deliver msg {}: {}'.format(
    ...                             msg.partition_key, repr(exc))
    ...                     else:
    ...                         print 'Successfully delivered msg {}'.format(
    ...                         msg.partition_key)
    ...                 except Queue.Empty:
    ...                     break

Note that the delivery report queue is thread-local: it will only serve reports
for messages which were produced from the current thread. Also, if you're using
`delivery_reports=True`, failing to consume the delivery report queue will cause
PyKafka's memory usage to grow unbounded.

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

You can also use the Kafka 0.9 Group Membership API with the `managed`
keyword argument on `get_balanced_consumer`.

Using the librdkafka extension
------------------------------

PyKafka includes a C extension that makes use of librdkafka to speed up producer
and consumer operation. To use the librdkafka extension, you need to make sure the header
files and shared library are somewhere where python can find them, both when you build
the extension (which is taken care of by ``setup.py develop``) and at run time.
Typically, this means that you need to either install librdkafka in a place
conventional for your system, or declare ``C_INCLUDE_PATH``, ``LIBRARY_PATH``,
and ``LD_LIBRARY_PATH`` in your shell environment.

After that, all that's needed is that you pass an extra parameter
``use_rdkafka=True`` to ``topic.get_producer()``,
``topic.get_simple_consumer()``, or ``topic.get_balanced_consumer()``.  Note
that some configuration options may have different optimal values; it may be
worthwhile to consult librdkafka's `configuration notes`_ for this.

We currently test against librdkafka `0.9.0.99`_ only.  Note that use on pypy is
not recommended at this time; the producer is certainly expected to crash.

.. _0.9.0.99: https://github.com/edenhill/librdkafka/releases/tag/0.9.0.99
.. _configuration notes: https://github.com/edenhill/librdkafka/blob/0.9.0.99/CONFIGURATION.md

Operational Tools
-----------------

PyKafka includes a small collection of `CLI tools`_ that can help with common tasks
related to the administration of a Kafka cluster, including offset and lag monitoring and
topic inspection. The full, up-to-date interface for these tools can be fould by running

.. sourcecode:: sh

    $ python cli/kafka_tools.py --help

or after installing PyKafka via setuptools or pip:

.. sourcecode:: sh

    $ kafka-tools --help

.. _CLI tools: https://github.com/Parsely/pykafka/blob/master/pykafka/cli/kafka_tools.py

What happened to Samsa?
-----------------------

This project used to be called samsa. It has been renamed PyKafka and has been
fully overhauled to support Kafka 0.8.2. We chose to target 0.8.2 because the offset
Commit/Fetch API stabilized on that release.

The Samsa `PyPI package`_  will stay up for the foreseeable future and tags for
previous versions will always be available in this repo.

.. _PyPI package: https://pypi.python.org/pypi/samsa/0.3.11

PyKafka or kafka-python?
------------------------

These are two different projects.
See `the discussion here <https://github.com/Parsely/pykafka/issues/334>`_ for comparisons
between the two projects.

Contributing
------------

If you're interested in contributing code to PyKafka, a good place to start is the
`"help wanted"`_ issue tag. We also recommend taking a look at the `contribution guide`_.

.. _"help wanted": https://github.com/Parsely/pykafka/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22
.. _contribution guide: https://github.com/Parsely/pykafka/blob/master/CONTRIBUTING.rst

Support
-------

If you need help using PyKafka or have found a bug, please open a `github issue`_ or use the `Google Group`_.

.. _github issue: https://github.com/Parsely/pykafka/issues
.. _Google Group: https://groups.google.com/forum/#!forum/pykafka-user
