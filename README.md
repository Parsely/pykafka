# samsa

A cluster-aware Kafka client and routing library for Python.

## What is Kafka?

Kafka is a distributed publish-subscribe messaging system. Cluster management
happens with ZooKeeper.

* Brokers
* Topics
* Partitions
* Messages

The clients of a Kafka cluster are generally split into two different categories:

* Producer
* Consumer

## Goals

* Your application code should not be dependent on broker state or Kafka cluster
  configuration. The only service that it should know about directly is the
  ZooKeeper cluster.
* Provide a level of abstraction and functionality similar to that of the Scala
  client, but using Pythonic idioms and native data structures.

## Guarantees

* Every publish will be written to (at least) one node, as long as a node is up
  to accept the write.

## Usage

### Creating a Cluster

    zookeeper = KazooClient()
    cluster = Cluster(zookeeper)

### Brokers

To list the brokers in the cluster:

    >>> print cluster.brokers.keys()
    [0, 1]

### Topics

The cluster provides a dictionary-like interface for retrieving `Topic` objects
based on their `name`. For example,

    >>> cluster.topics['topic-name']
    <Topic: topic-name>

You can also list all registered topics by name:

    >>> print cluster.topics.keys()
    ['topic-name', 'another-topic']

You can also see how many partitions are available within a topic:

    >>> print cluster.topics['topic'].partitions
    [<Partition: 0-1>, <Partition: 1-1>]

If a topic is not available, it'll be automatically created when you access it:

    >>> print 'topic' in cluster.topics.key()
    False
    >>> topic = cluster.topics['topic']
    <Topic: topic>

...but if you go to look at partitions on a topic that doesn't have any yet, it
won't have any (until you start writing to it).

    >>> topic.partitions
    []

### Publishing

When publishing messages, you can either send them individually as a `Message`
or grouped together as a `MessageSet`.

    >>> Message('Hello world!')
    <Message>

    >>> MessageSet([Message('foo'), Message('bar')])
    <MessageSet: 2>

Within a `MessageSet`, all messages will be delivered to the same partition, in
order, on the same broker. Individual messages hold no such guarantee, and are
subject to end up on any number of hosts as determined by the partitioning
method, and can be distributed across a number of partitions, losing any strong
ordering semantics between messages.

You can publish messages to the cluster by passing a `Topic` object,

    >>> cluster.publish(topic, message)
    >>> cluster.publish(topic, message_set)

or, you can publish directly from the `Topic` object itself,

    >>> cluster.topics['topic'].publish(message)
    >>> cluster.topics['topic'].publish(message_set)

#### Semantic Partitioning

Provide the `key` argument to `publish`, and it will be used to attempt to
direct all messages with that key to the same partition. This is subject to
cluster rebalances, etc.

For example, all of these messages will be directed at the same partitions,
given that brokers (or partitions) are not added or removed between writes:

    >>> data = {'user': 1, 'event': 'logout'}
    >>> topic.publish(Message(json.dumps(data)), key=data['user'])

    >>> user = 1
    >>> data = [{'user': user, 'event': 'create'}, {'user': user, 'event': 'login'}]
    >>> messages = map(Message, map(json.dumps, data))
    >>> topic.publish(MessageSet(messages=messages), key=user)

### Batch Publishing Messages

Creating batches of messages allows you to buffer writes in memory, requiring
less network round trips if you are sending many messages.

    >>> batch = cluster.batch()
    >>> batch.append(topic, Message('Hello world'))
    >>> batch.append(topic, MessageSet(...))
    >>> batch.flush()  # Sends all of the messages in the batch.

or, using context managers:

    >>> with cluster.batch() as batch:
    ...     batch.append(topic, Message)
    ...     batch.append(topic, MessageSet)

(TODO: Add batch sizes for implicit flushes if the buffer grows too large.)

#### Batch Publishing Messages to a Single Topic

If you're going to send all messages on a batch to a single topic, you can use
the `batch` method on a `Topic` instead:

    >>> topic = cluster.topics['topic']
    >>> with topic.batch() as batch:
    ...     batch.append(Message('Hello world'))
    ...     batch.append(MessageSet(...))

The same non-context manager syntax applies as with `Cluster.batch()`.

#### Semantic Partitioning with Batch Publishing

To add semantic paritioning information to batches, just provide a `key`
argument to `append` as you would with `publish` on either a `Cluster` or a
`Topic`. For example,

    >>> with cluster.batch() as batch:
    ...     batch.append(topic, message=Message(...), key='my-key')
    ...     batch.append(topic, message=MessageSet(...), key='my-other-key')

### Consuming Messages

Consumers are organized into consumer groups.

This allows Kafka to provide two semantically different methods of message
consumption, based on the consumer group configuration.

* Queue: Each consumer in a consumer group receives a message once, which
  provides for a reasonably even distribution of messages around the group 
  since it is the owner of only a subset of the available partitions within
  the topic. This is similar to a put/get queue or many AMQP-like interfaces.
* Topic: Each consumer is it's own consumer group, and receives all messages
  sent to the topic since it is the owner of all partitions. This is similar to
  a evented system, or a publish/subscribe interface.

#### A Note about Partition Counts

Also, an important thing to note is that if there are more consumers than
partitions, some consumers won't get any data at all, so you should configure
your brokers up front to break topics into at least as many partitions as you
expect to have consumers. To get the minimum number of partitions you should
make available, use the following equation:

    # of brokers * # of partitions per node = # of consumers in the largest consumer group for this topic

#### Creating Consumers

To create a new consumer,

    >>> consumer = cluster.consumer(topic, group='my-group')

or, if the topic is more your style,

    >>> topic = cluster.topics['topic']
    >>> consumer = topic.consumer(group='my-group')

Consumers have to be registered with ZooKeeper before they begin to receive
messages. (Remember, only one consumer in a consumer group can be reading from
a partition at any time.)

To listen for messages on an established consumer,

    >>> for message in consumer.subscribe():
    ...     print message

TODO: Single get?

#### Partition Offsets

TODO: Default offsets
TODO: Providing an explicit offset?
