Changelog
=========

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
