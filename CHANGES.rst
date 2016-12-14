Changelog
=========

2.6.0.dev2 (2016-12-14)
-----------------------

`Compare 2.6.0.dev2`_

.. _Compare 2.6.0.dev2: https://github.com/Parsely/pykafka/compare/2.6.0.dev1...2.6.0.dev2

Bug Fixes
---------

* Handled `struct.error` exceptions in `Producer._send_request`

Miscellaneous
-------------

* Upgraded the version of PyPy used in automated tests

2.6.0.dev1 (2016-12-8)
----------------------

`Compare 2.6.0.dev1`_

.. _Compare 2.6.0.dev1: https://github.com/Parsely/pykafka/compare/2.5.0...2.6.0.dev1

Minor Version Features
----------------------

* Added support to `Broker` and `Cluster` for Kafka 0.10's Administrative API
* Changed the `MemberAssignment` protocol API to more closely match the schema defined
  by Kafka

Bug Fixes
---------

* Changed components to use `six.reraise` to raise worker thread exceptions for easier
  debugging
* Included message offset in messages returned from `Producer` delivery reports
* Changed protocol implementation to parse `ConsumerGroupProtocolMetadata` from
  bytestrings returned from Kafka

2.5.0 (2016-9-15)
-----------------

`Compare 2.5.0`_

.. _Compare 2.5.0: https://github.com/Parsely/pykafka/compare/2.4.0...2.5.0

Minor version Features
----------------------

* Added the `broker_version` kwarg to several components. It's currently only
  used by the librdkafka features. The kwarg is used to facilitate the use of
  librdkafka via pykafka against multiple Kafka broker versions.
* Changed offset commit requests to include useful information in the offset
  metadata field, including consumer ID and hostname
* Added the `GroupHashingPartitioner`

Bug Fixes
---------

* Fixed the operation of `consumer_timeout_ms`, which had been broken for
  `BalancedConsumer` groups
* Fixed a bug causing `Producer.__del__` to crash during finalization
* Made the consumer's fetch loop nonbusy when the internal queues are full to
  save CPU cycles when message volume is high
* Fixed a bug causing `Producer.flush()` to wait for `linger_ms` during calls initiated
  by `_update()`
* Fixed a race condition between `Producer._update` and `OwnedBroker.flush` causing
  infinite retry loops
* Changed `Producer.produce` to block while the internal broker list is being updated.
  This avoids possible mismatches between old and new cluster metadata used by the
  `Producer`.
* Fixed an issue causing consumer group names to be written to ZooKeeper with a literal
  `b''` in python3. :warning:**Since this change adjusts ZooKeeper storage formats, it
  should be applied with caution to production systems. Deploying this change without a
  careful rollout plan could cause consumers to lose track of their offsets.**:warning:
* Added logic to group coordinator discovery that retries the request once per broker
* Handled socket errors in `BrokerConnection`
* Fixed a bug causing synchronous production to hang in some situations

Miscellaneous
-------------

* Upgraded the version of PyPy used in automated tests
* Upgraded the version of librdkafka used in automated tests
* Pinned the version of the `testinstances` library on which the tests depend

2.5.0.dev1 (2016-8-23)
----------------------

`Compare 2.5.0.dev1`_

.. _Compare 2.5.0.dev1: https://github.com/Parsely/pykafka/compare/2.4.1.dev1...2.5.0.dev1

You can install this release via pip with `pip install --pre pykafka==2.5.0.dev1`.
It will not automatically install because it's a pre-release.

Minor version Features
----------------------

* Added the `broker_version` kwarg to several components. It's currently only
  used by the librdkafka features. The kwarg is used to facilitate the use of
  librdkafka via pykafka against multiple Kafka broker versions.
* Changed offset commit requests to include useful information in the offset
  metadata field, including consumer ID and hostname
* Added the `GroupHashingPartitioner`

Bug Fixes
---------

* Fixed the operation of `consumer_timeout_ms`, which had been broken for
  `BalancedConsumer` groups
* Fixed a bug causing `Producer.__del__` to crash during finalization
* Made the consumer's fetch loop nonbusy when the internal queues are full to
  save CPU cycles when message volume is high
* Fixed a bug causing `Producer.flush()` to wait for `linger_ms` during calls initiated
  by `_update()`
* Fixed a race condition between `Producer._update` and `OwnedBroker.flush` causing
  infinite retry loops
* Changed `Producer.produce` to block while the internal broker list is being updated.
  This avoids possible mismatches between old and new cluster metadata used by the
  `Producer`.

Miscellaneous
-------------

* Upgraded the version of PyPy used in automated tests
* Upgraded the version of librdkafka used in automated tests
* Pinned the version of the `testinstances` library on which the tests depend

2.4.1.dev1 (2016-7-6)
---------------------

`Compare 2.4.1.dev1`_

.. _Compare 2.4.1.dev1: https://github.com/Parsely/pykafka/compare/2.4.0...2.4.1.dev1

You can install this release via pip with `pip install --pre pykafka==2.4.1.dev1`.
It will not automatically install because it's a pre-release.

Bug Fixes
---------

* Fixed an issue causing consumer group names to be written to ZooKeeper with a literal
  `b''`. :warning:**Since this change adjusts ZooKeeper storage formats, it should be applied with
  caution to production systems. Deploying this change without a careful rollout plan
  could cause consumers to lose track of their offsets.**:warning:
* Added logic to group coordinator discovery that retries the request once per broker
* Handled socket errors in `BrokerConnection`
* Fixed a bug causing synchronous production to hang in some situations

2.4.0 (2016-5-25)
-----------------

`Compare 2.4.0`_

.. _Compare 2.4.0: https://github.com/Parsely/pykafka/compare/2.3.1...2.4.0

Minor Version Features
**********************

* Added support for connecting to Kafka brokers using a secure TLS connection
* Removed the fallback in `Cluster` that treated `hosts` as a ZooKeeper
  connection string
* Removed the `block_on_queue_full` kwarg from the rdkafka producer
* Added the `max_request_size` kwarg to the rdkafka producer

Bug Fixes
*********

* Performed permissive parameter validation in consumers and producer to avoid
  cryptic errors on threads
* Allowed more consumers than partitions in a balanced consumer group
* Fixed python 3 compatibility in `kafka_tools.py`
* Fixed a bug causing nuisance errors on interpreter shutdown
* Removed some uses of deprecated functions in the rdkafka C extension
* Fixed a bug causing crashes when kafka returns an invalid partition ID in
  partition requests

Miscellaneous
*************

* Added utilities for testing TLS support to the test suite
* Made the gevent version requirement slightly more inclusive


2.3.1 (2016-4-8)
----------------

`Compare 2.3.1`_

.. _Compare 2.3.1: https://github.com/parsely/pykafka/compare/2.3.0...4fb854cc5a7cba11ea58329a4a336edc38a5a3bd

Bug Fixes
*********

* Fixed a `NoneType` crash in `Producer` when rejecting larger messages
* Stopped `Producer` integration tests from sharing a `Consumer` instance to make test
  runs more consistent

Miscellaneous
*************

* Added warning about using Snappy compression under PyPy
* Clarified language around "most recent offset available"

2.3.0 (2016-3-22)
-----------------

`Compare 2.3.0`_

.. _Compare 2.3.0: https://github.com/Parsely/pykafka/compare/2.2.1...7855fa2beeb08c0f35a343d4f9ba09c725cdd32f

Minor Version Features
**********************

* Added the `ManagedBalancedConsumer` class, which performs balanced consumption
  using the Kafka 0.9 Group Membership API
* Added the `managed` keyword argument to `Topic.get_balanced_consumer` to access
  `ManagedBalancedConsumer`
* Added a `compacted_topic` kwarg to `BalancedConsumer` to make it smarter about
  offset ordering for compacted topics
* Added methods to `Broker` that use the Group Membership API
* Changed the terminology "offset manager" to "group coordinator" to match updated
  Kafka jargon
* Added new exception types from Kafka 0.9
* Added `auto_start` keyword argument to `Producer` to match the consumer interface
* Added `max_request_size` keyword argument to `Producer` to catch large messages
  before they're sent to Kafka
* Added protocol functions for the Group Membership API
* New `SimpleConsumer` keyword arguments: `compacted_topic`, `generation_id`,
  `consumer_id`

Bug Fixes
*********

* Fixed a bug in Travis config causing tests not to run against Kafka 0.9
* Upgraded to non-beta gevent version
* Allowed a single `Broker` instance to maintain multiple connections to a broker
  (useful when multiple consumers are sharing the same `KafkaClient`)
* Allowed switchable socket implementations when using gevent
* Handled `TypeError` during worker thread shutdown to avoid nuisance messages
* Limited `Producer.min_queued_messages` to 1 when `sync=True`
* Monkeypatched a bug in py.test causing tests to be erroneously skipped

Miscellaneous
*************

* Added an issue template


2.2.1 (2016-2-19)
-----------------

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
