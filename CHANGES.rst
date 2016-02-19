Changelog
=========

2.2.1 (2016-2-19)
----------------

`Compare 2.2.1`_

.. _Compare 2.2.1: https://github.com/Parsely/pykafka/compare/2.2.0...538c476d876df09c71496b82c4ac6a2f720c6765

Bug Fixes
*********

* Fixed Travis issues related to PyPy testing
* Fixed deprecated dependency on gevent.coros
* Enabled caching in Travis for pip, librdkafka, and kafka installations
* Fixed a crash during metadata updating related to zookeeper fallback
* Unified connection retry logic in `Cluster`
* Raised an exception if consumer offset reset fails after maximum retries
* Fixed a bug allowing `get_delivery_report` to indefinitely block `produce()`
* Fixed a bug causing producers to drop `to_retry` messages on `stop()`
* Added retry logic to offset limit fetching


2.2.0 (2016-1-26)
----------------

`Compare 2.2.0`_

.. _Compare 2.2.0: https://github.com/Parsely/pykafka/compare/2.1.2...c1174cf6f67d350d279cf292fd7d9be9c9767600

Minor Version Features
**********************

* Added support for gevent-based concurrency in pure cpython
* Allowed ZooKeeper hosts to be specified directly to KafkaClient instead of
  being treated as a fallback


Bug Fixes
*********

* Fixed a bug causing `RLock`-related crashes in Python 3
* Used the more stable sha1 hash function as the default for
  `hashing_partitioner`
* Fixed a bug in the meaning of `linger_ms` in the producer



2.1.2 (2016-1-8)
----------------

`Compare 2.1.2`_

.. _Compare 2.1.2: https://github.com/Parsely/pykafka/compare/2.1.1...70cce0fb59f4d0f6a4e50bb7521d2edb9c1e66fa

Features
********

* Allowed consumers to run with no partitions

Bug Fixes
*********

* Fixed a bug causing consumers to hold outdated partition sets
* Handled some previously uncaught error codes in `SimpleConsumer`
* Fixed an off-by-one bug in message set fetching
* Made `consume()` stricter about message ordering and duplication


2.1.1 (2015-12-11)
------------------

`Compare 2.1.1`_

.. _Compare 2.1.1: https://github.com/Parsely/pykafka/compare/2.1.0...e5c320d60246f98afda458b7c7c43dc2c428de46

Features
********

* Improved unicode-related error reporting in several components
* Removed the ZooKeeper checker thread from the `BalancedConsumer`
* Added a test consumer CLI to `kafka_tools`


Bug Fixes
*********

* Fixed a memory leak in the rdkafka-based consumer
* Fixed offset committing to work against Kafka 0.9
* Improved the reliability of the Kafka test harness

Miscellaneous
*************

* Simplified the Travis test matrix to handle testing against multiple Kafka versions


2.1.0 (2015-11-25)
------------------

`Compare 2.1.0`_

.. _Compare 2.1.0: https://github.com/Parsely/pykafka/compare/2.0.4...468d10cff6f07c4dff59535618c42f84b93d9b7d

Features
********

* Addded an optional C extension making use of librdkafka for enhanced producer and
  consumer performance
* Added a delivery report queue to the `Producer` allowing per-message errors
  to be handled
* Added a callback indicating that the `BalancedConsumer` is in the process of rebalancing

Bug Fixes
*********

* Fixed a longstanding issue causing certain tests to hang on Travis
* Fixed a bug causing the default error handles in the consumer to mask unknown error
  codes
* Moved the `Message` class to using `__slots__` to minimize its memory footprint


2.0.4 (2015-11-23)
------------------

`Compare 2.0.4`_

.. _Compare 2.0.4: https://github.com/Parsely/pykafka/compare/2.0.3...a3e6398c6b5291f189f4cc3de66c1cb7f160564c

Features
********

* Allowed discovery of Kafka brokers via a ZooKeeper connect string supplied to
  `KafkaClient`

Bug Fixes
*********

* Made `BalancedConsumer`'s ZooKeeper watches close quietly on consumer exit
* Disconnect sockets in response to any socket-level errors
* Fixed `HashingPartitioner` for python 3

2.0.3 (2015-11-10)
------------------

`Compare 2.0.3`_

.. _Compare 2.0.3: https://github.com/Parsely/pykafka/compare/2.0.2...bd844cd66e79b3e0a56dd92a2aae4579a9046e8e

Features
********

* Raise exceptions from worker threads to the main thread in `BalancedConsumer`
* Call `stop()` when `BalancedConsumer` is finalized to minimize zombie threads

Bug Fixes
*********

* Use weak references in `BalancedConsumer` workers to avoid zombie threads creating
  memory leaks
* Stabilize `BalancedConsumer.start()`
* Fix a bug in `TopicDict.values()` causing topics to be listed as `None`
* Handle `IOError` in `BrokerConnection` and `socket.recvall_into`
* Unconditionally update partitions' leaders after metadata requests
* Fix thread-related memory leaks in `Producer`
* Handle connection errors during offset commits
* Fix an interpreter error in `SimpleConsumer`

2.0.2 (2015-10-29)
------------------

`Compare 2.0.2`_

.. _Compare 2.0.2: https://github.com/Parsely/pykafka/compare/2.0.1...75276e361ec546777f2fad6dae72f2e1125c0ec9

Features
********

* Switched the `BalancedConsumer` to using ZooKeeper as the single source of truth
  about which partitions are held
* Made `BalancedConsumer` resilient to ZooKeeper failure
* Made the consumer resilient to broker failure

Bug Fixes
*********

* Fixed a bug in `BrokerConnection` causing the message length field to
  occasionally be corrupted
* Fixed a bug causing `RequestHandler` worker threads to sometimes abort
  before the request was completed
* Fixed a bug causing `SimpleConsumer` to hang when the number of brokers in
  the cluster goes below the replication factor

2.0.1 (2015-10-19)
------------------

`Compare 2.0.1`_

.. _Compare 2.0.1: https://github.com/Parsely/pykafka/compare/2.0.0...b01c62b7b512776dcb9822a8f3b785f5e65da3ab

Features
********

* Added support for python 3.5
* Added iteration to the `BalancedConsumer`
* Disallowed `min_queued_messages<1` in `Producer`
* Made `SimpleConsumer` commit offsets on `stop()`
* Supported `None` in message values

Bug Fixes
*********

* Improved `BalancedConsumer`'s handling of an external `KazooClient` instance
* Fixed `kafka_tools.py` for Python 3
* Removed the unused `use_greenlets` kwarg from `KafkaClient`
* Improved `Cluster`'s ability to reconnect to brokers during metadata updates
* Fixed an interpreter error in `conncection.py`
* Fixed failure case in `Producer` when `required_acks==0`
* Fixed a bug causing `SimpleConsumer` to leave zombie threads after disconnected brokers
* Improved `SimpleConsumer`'s worker thread exception reporting
* Simplified `SimpleConsumer`'s partition locking logic during `fetch` by using `RLock`
* Fixed `SimpleConsumer` off-by-one error causing lag to never reach 0

Miscellaneous
*************

* Switched from Coveralls to Codecov for converage tracking

2.0.0 (2015-09-25)
------------------

`Compare 2.0.0`_

.. _Compare 2.0.0: https://github.com/Parsely/pykafka/compare/12f522870a32198f70a92ce543950c88b9f75565...ad0d06bd4acbe95cdfa8dfe858dfab5d162a4d09

Features
********

* Rewrote the producer in an asynchronous style and made small breaking changes to its interface. Specifically, it doesn't accept sequences of messages anymore - only one message at a time.
* Made the entire library compatible with python 3.4, 2.7, and PyPy, and adopted Tox as the test runner of choice.
* Allowed the socket source address to be specified when instantiating a client
* Started a usage guide and contribution guide in the documentation

Bug Fixes
*********

* Fixed unnecessarily long rebalance loops in the `BalancedConsumer`
* Fixed hanging consumer integration tests
* Fixed a bug causing the client's thread workers to become zombies under certain conditions
* Many miscellaneous bugfixes

1.0.0 (2015-05-31)
------------------

Features
********

Completely re-wrote almost everything and renamed to PyKafka.


0.3.8 (2013-07-30)
------------------

Features
********

- `Topic.latest_offsets` and `Partition.latest_offset` convenience functions

- Test cases are now significantly faster, but still deadlocking in Travis-CI

Bug Handling
************

- Issue #93: deal with case where kazoo is passed in not-connected

- Issue #91: offsets can get corrupted

- Handle race condition where zookeeper gave None for broker information
  after it had been removed

- Pin kazoo to v1.1 because 1.2 is broken in PYPI


0.3.6 (2013-04-30)
------------------

Features
********

- Improved partition queueing. Won't wait when there are partitions with data.

- Production-ready rebalancing. Refactoring and bug fixing resulting in greater
  stability when adding and removing consumers and eliminated known race
  conditions.

Bug Handling
************

- Issue #80: `decode_messages` crashes when payload ends in a header

- Issue #79: unexpected "Couldn't acquire partitions error"

- Issue #78: unexpected "sample larger than population" error

- Issue #77: prevent integration tests from starting before zookeeper cluster
  and kafka broker are ready

- Issue #76: test for "more workers than queues" in partitioner

- Issue #68: All watches should use the DataWatch recipe

- Issue #62: Dead lock when consumer timeout is None and no messages
