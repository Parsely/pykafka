Changelog
=========

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
