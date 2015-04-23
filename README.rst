![Build Status](https://travis-ci.org/Parsely/pykafka.svg?branch=master)](https://travis-ci.org/Parsely/pykafka)

# What happened to Samsa?

Samsa has been renamed PyKafka in anticipation of our next release, which will
include support for Kafka 0.8.2 as well as optional integration with
librdkafka in order to be that much faster.

The [PyPI package](https://pypi.python.org/pypi/samsa/0.3.11) will stay up for
the foreseeable future and tags for previous versions will always be
available in this repo.

# PyKafka 0.8.2-01 Plan

The goals of this release are:

  * Rename samsa to pykafka
  * Add 0.8.2 support to pykafka
  * Support speedups via librdkafka

To view the status of this release, check out the
[milestone](https://github.com/Parsely/pykafka/milestones/0.8.2-01).

## Adding Kafka 0.8.2 support to PyKafka

We chose to target 0.8.2 because the Offset Commit/Fetch API is stabilized
and we anticipate 0.8.2 being fully released by the time this version is ready.

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
