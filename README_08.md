# PyKafka 0.8 (with librdkafka) Overview

The goals of this branch are:

  * Rename samsa to pykafka
  * Add 0.8.2 support to pykafka
  * Support speedups via librdkafka

## Rename samsa to pykafka

It's a better name, and avoid confustion with samza.  Still need to do this.

## Adding 0.8.2 support to pykafka

We chose to target 0.8.2 because the Offset Commit/Fetch API is stabilized
and we anticipate 0.8.2 being fully released by the time this version is ready.

Items Complete:
  * Architect new zookeeper-free object model
  * Abstract model to accommodate librdkafka integration
  * Implement 0.8 wire protocol (minus offset commit/fetch)
  * Create object model of cluster based on broker-provided metadata
  * Partition offset fetch implementation
  * Producer implementation

Still be to done:
  * Offset commit/fetch wire protocol implementation
  * Offset commit/fetch integrated into consumers
    * Replaces zookeeper offset storage
  * [inprog] SimpleConsumer implementation
  * BalancedConsumer implementation
    * Ideally, will be agnostic between C and Python implementations
  * Test coverage
  * Ensure driver reconfigures seamlessly when cluster topology changes

## librdkafka speedups

[librdkafka](https://github.com/edenhill/librdkafka) is a C/C++ driver for
Kafka. One of the goals of this branch is to integrate
[python-librdkafka](https://bitbucket.org/yungchin/python-librdkafka) with
pykafka in order to speed up the driver. However, like `hiredis` and `redis`,
`python-librdkafka` will be used to speed up pykafka, but not be required by it.

### librdkafka architecture

Since `librdkafka` is a very full-featured driver, the split between the Python
and C implementations exists at a very high level.  This can be seen in
[client.py](samsa/client.py) and [abstract.py](samsa/abstract.py) where the latter
is the interface by which the client can work with either the Python or
C implementation.

This architecture is still a work in progress, so the approach may change as
time goes on.  We're still working through the implications of essentially
having two implementations in the same driver.

### librdkafka Integration Status

TODO: Fill in status updates
