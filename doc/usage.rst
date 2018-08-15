PyKafka Usage Guide
===================

This document contains explanations and examples of common patterns of PyKafka usage.

Consumer Patterns
=================

Setting the initial offset
--------------------------

When a PyKafka consumer starts fetching messages from a topic, its starting position in
the log is defined by two keyword arguments: `auto_offset_reset` and
`reset_offset_on_start`.

.. sourcecode:: python

    consumer = topic.get_simple_consumer(
        consumer_group="mygroup",
        auto_offset_reset=OffsetType.EARLIEST,
        reset_offset_on_start=False
    )

The starting offset is also affected by whether or not the Kafka cluster holds any
previously committed offsets for each consumer group/topic/partition set. In this
document, a "new" group/topic/partition set is one for which Kafka does not hold any
previously committed offsets, and an "existing" set is one for which Kafka does.

The consumer's initial behavior can be summed up by these rules:

- For any *new* group/topic/partitions, message consumption will start from
  `auto_offset_reset`. This is true independent of the value of `reset_offset_on_start`.
- For any *existing* group/topic/partitions, assuming `reset_offset_on_start=False`,
  consumption will start from the offset
  immediately following the last committed offset (if the last committed offset was
  4, consumption starts at 5). If `reset_offset_on_start=True`, consumption starts from
  `auto_offset_reset`. If there is no committed offset, the group/topic/partition
  is considered *new*.

Put another way: if `reset_offset_on_start=True`, consumption will start from
`auto_offset_reset`. If it is `False`, where consumption starts is dependent on the
existence of committed offsets for the group/topic/partition in question.

Examples:

.. sourcecode:: python

    # assuming "mygroup" has no committed offsets

    # starts from the latest available offset
    consumer = topic.get_simple_consumer(
        consumer_group="mygroup",
        auto_offset_reset=OffsetType.LATEST
    )
    consumer.consume()
    consumer.commit_offsets()

    # starts from the last committed offset
    consumer_2 = topic.get_simple_consumer(
        consumer_group="mygroup"
    )

    # starts from the earliest available offset
    consumer_3 = topic.get_simple_consumer(
        consumer_group="mygroup",
        auto_offset_reset=OffsetType.EARLIEST,
        reset_offset_on_start=True
    )

This behavior is based on the `auto.offset.reset` section of the `Kafka documentation`_.

.. _Kafka documentation: http://kafka.apache.org/documentation.html

Consuming the last N messages from a topic
------------------------------------------

When you want to see only the last few messages of a topic, you can use the following
pattern.

.. sourcecode:: python


    from __future__ import division

    import math
    from itertools import islice

    from pykafka import KafkaClient
    from pykafka.common import OffsetType

    client = KafkaClient()
    topic = client.topics['mytopic']
    consumer = topic.get_simple_consumer(
        auto_offset_reset=OffsetType.LATEST,
        reset_offset_on_start=True)
    LAST_N_MESSAGES = 50
    # how many messages should we get from the end of each partition?
    MAX_PARTITION_REWIND = int(math.ceil(LAST_N_MESSAGES / len(consumer._partitions)))
    # find the beginning of the range we care about for each partition
    offsets = [(p, op.last_offset_consumed - MAX_PARTITION_REWIND)
               for p, op in consumer._partitions.iteritems()]
    # if we want to rewind before the beginning of the partition, limit to beginning
    offsets = [(p, (o if o > -1 else -2)) for p, o in offsets]
    # reset the consumer's offsets
    consumer.reset_offsets(offsets)
    for message in islice(consumer, LAST_N_MESSAGES):
        print(message.offset, message.value)


`op.last_offset_consumed` is the "head" pointer of the consumer instance. Since we start by
setting this consumer to `LATEST`, `last_offset_consumed` is the latest offset for the
partition.  Thus, `last_offset_consumed - MAX_PARTITION_REWIND` gives the starting
offset of the last messages per partition.

Producer Patterns
=================

Producing to multiple topics
----------------------------

Avoid repeated calls to the relatively `get_producer` when possible. If producing to
multiple topics from a single process, it's helpful to keep the `Producer` objects in
memory instead of letting them be garbage collected and reinstantiated repeatedly.

.. sourcecode:: python

    topic_producers = {topic.name: topic.get_producer() for topic in topics_to_produce_to}
    for destination_topic, message in consumed_messages:
        topic_producers[destination_topic.name].produce(message)


Handling connection loss
========================

The pykafka components are designed to raise exceptions when sufficient connection to
the Kafka cluster cannot be established. There are cases in which some but not all of
the brokers in a cluster are accessible to pykafka. In these cases, the component will
attempt to continue operating. When it can't, an exception will be raised. Often this
exception will be either `NoBrokersAvailableError` or `SocketDisconnectedError`. These
exceptions should be caught and the component instance should be reinstantiated. In some
cases, calling `stop(); start()` in response to these exceptions can be enough to
establish a working connection.

.. sourcecode:: python

    from pykafka.exceptions import SocketDisconnectedError, NoBrokersAvailableError
    # this illustrates consumer error catching; a similar method can be used for producers
    consumer = topic.get_simple_consumer()
    try:
        consumer.consume()
    except (SocketDisconnectedError, NoBrokersAvailableError) as e:
        consumer = topic.get_simple_consumer()
        # use either the above method or the following:
        consumer.stop()
        consumer.start()

