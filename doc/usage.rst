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

