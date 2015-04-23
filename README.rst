.. image:: https://travis-ci.org/Parsely/pykafka.svg?branch=master

What happened to Samsa?
=======================

Samsa has been renamed PyKafka in anticipation of our next release, which will
include support for Kafka 0.8.2 as well as optional integration with
librdkafka in order to be that much faster.

The `PyPI package`_  will stay up for the foreseeable future and tags for
previous versions will always be available in this repo.

.. _PyPI package: https://pypi.python.org/pypi/samsa/0.3.11

PyKafka 0.8.2-01 Plan
---------------------

The goals of this release are:

  * Rename samsa to pykafka
  * Add 0.8.2 support to pykafka

To view the status of this release, check out the
`milestone`_.

.. _milestone: https://github.com/Parsely/pykafka/milestones/0.8.2-01

Adding Kafka 0.8.2 support to PyKafka
-------------------------------------

We chose to target 0.8.2 because the Offset Commit/Fetch API is stabilized
and we anticipate 0.8.2 being fully released by the time this version is ready.
