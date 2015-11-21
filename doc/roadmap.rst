Kafka 0.9 Roadmap for PyKafka
=============================

Date: November 20, 2015

Quick summary
-------------

The current stable version of Kafka is 0.8.2. This is meant to run
against the latest Zookeeper versions, e.g. 3.4.6.

The latest releases of pykafka target 0.8.2 **specifically**; the Python
code is not backwards compatible with 0.8.1 due to changes in what is
known as Offset Commit/Fetch API, which pykafka uses to simplify the
offset management APIs and standardize them with other clients that talk
to Kafka.

The 0.8.2 release will likely be the most stable Kafka broker to use in
production for the next couple of months. However, as we will discuss
later, there is a specific bug in Kafka brokers that was fixed in 0.9.0
that we may find advantageous to backport to 0.8.2.

Meanwhile, 0.9.0 is "around the corner" (currently in release candidate
form) and introduces, yet again, a brand new consumer API, which we need
to track and wrap in pykafka. But for that to stabilize will take some
time.

SimpleConsumer vs BalancedConsumer
----------------------------------

Why does pykafka exist? That's a question I sometimes hear from people,
especially since there are alternative implementations of the Kafka
protocol floating around in the Python community, notably
`kafka-python <https://github.com/mumrah/kafka-python>`__.

One part of the reason pykafka exists is to build a more Pythonic API
for working with Kafka that supports every major Python interpreter
(Python 2/3, PyPy) and every single Kafka feature. We also have an
interest in making Kafka consumers fast, with C optimizations for
protocol speedups. But the **real** reason it exists is to implement a
**scalable and reliable BalancedConsumer** implementation atop Kafka and
Zookeeper. This was missing from any Kafka and Python project, and we
(and many other users) desperately needed it to use Kafka in the way it
is meant to be used.

Since there is some confusion on this, let's do a crystal clear
discussion of the differences between these two consumer types.

**SimpleConsumer** communicates **directly** with a Kafka broker to
consume a Kafka topic, and takes "ownership" of 100% of the partitions
reported for that topic. It does round-robin consumption of messages
from those partitions, while using the aforementioned Commit/Fetch API
to manage offsets. Under the hood, the Kafka broker talks to Zookeeper
to maintain the offset state.

The main problems with SimpleConsumer: scalability, parallelism, and
high availability. If you have a busy topic with lots of partitions, a
SimpleConsumer may not be able to keep up, and your offset lag (as
reported by kafka-tools) will constantly be behind, or worse, may grow
over time. You may also have code that needs to react to messages, and
that code may be CPU-bound, so you may be seeking to achieve multi-core
or multi-node parallelism. Since a SimpleConsumer has no coordination
mechanism, you have no options here: multiple SimpleConsumer instances
reading from the same topic will read **the same messages** from that
topic -- that is, the data won't be spread evenly among the consumers.
Finally, there is the availability concern. If your SimpleConsumer dies,
your pipeline dies. You'd ideally like to have several consumers such
that the death of one does not result in the death of your pipeline.

One other side note related to using Kafka in Storm, since that's a
common use case. Typically Kafka data enters a Storm topology via a
Spout written against pykafka's API. If that Spout makes use of a
SimpleConsumer, you can only set that Spout's parallelism level to 1 --
a parallel Spout will emit duplicate tuples into your topology!

So, now let's discuss **BalancedConsumer** and how it solves these
problems. Instead of taking ownership of 100% partitions upon
consumption of a topic, a BalancedConsumer in Kafka 0.8.2 coordinates
the state for several consumers who "share" a single topic by talking to
the Kafka broker and directly to Zookeeper. It figures this out by
registering a "consumer group ID", which is an identifier associated
with several consumer processes that are all eating data from the same
topic, in a balanced manner.

The following discussion of the BalancedConsumer operation is very
simplified and high-level -- it's not exactly how it works. But it'll
serve to illustrate the idea. Let's say you have 10 partitions for a
given topic. A BalancedConsumer connects asks the cluster, "what
partitions are available?". The cluster replies, "10". So now that
consumer takes "ownership" of 100% of the partitions, and starts
consuming. At this moment, the BalancedConsumer is operating similarly
to a SimpleConsumer.

Then a **second** BalancedConsumer connects and the cluster, "which
partitions are available? Cluster replies, "0", and asks the
BalancedConsumer to wait a second. It now initiates a "partition
rebalancing". This is a fancy dance between Zookeeper and Kafka, but the
end result is that 5 partitions get "owned" by consumer A and 5 get
"owned" by consumer B. The original consumer receives a notification
that the partition balancing has changed, so it now consumes from fewer
partitions. Meanwhile, the second BalancedConsumer now gets a new
notification: "5" is the number of partitions it can now own. At this
point, 50% of the stream is being consumed by consumer A, and 50% by
consumer B.

You can see where this goes. A third, fourth, fifth, or sixth
BalancedConsumer could join the group. This would split up the
partitions yet further. However, note -- we mentioned that the total
number of partitions for this topic was 10. Thus, though balancing will
work, it will only work up to the number of total partitions available
for a topic. That is, if we had 11 BalancedConsumers in this consumer
group, we'd have one idle consumer and 10 active consumers, with the
active ones only consuming 1 partition each.

The good news is, it's very typical to run Kafka topics with 20, 50, or
even 100 partitions per topic, and this typically provides enough
parallelism and availability for almost any need.

Finally, availability is provided with the same mechanism. If you unplug
a BalancedConsumer, its partitions are returned to the group, and other
group members can take ownership. This is especially powerful in a Storm
topology, where a Spout using a BalancedConsumer might have parallelism
of 10 or 20, and single Spout instance failures would trigger
rebalancing automatically.

Pure Python vs rdkafka
----------------------

A commonly used Kafka utility is
`kafkacat <https://github.com/edenhill/kafkacat>`__, which is written by
Magnus Edenhill. It is written in C and makes use of the
`librdkafka <https://github.com/edenhill/librdkafka>`__ library, which
is a pure C wrapper for the Kafka protocol that has been benchmarked to
support 3 million messages per second on the consumer side. A member of
the Parse.ly team has written a pykafka binding for this library which
serves two purposes: a) speeding up Python consumers and b) providing an
alternative protocol implementation that allows us to isolate
protocol-level bugs.

Note that on the consumer side, librdkafka only handles direct
communication with the Kafka broker. Therefore, BalancedConsumer still
makes use of pykafka's pure Python Zookeeper handling code to implement
partition rebalancing among consumers.

Under the hood, librdkafka is wrapped using Python's C extension API,
therefore it adds a little C wrapper code to pykafka's codebase.
Building this C extension requires that librdkafka is already built and
installed on your machine (local or production).

By the end of November, rdkafka will be a fully supported option of
pykafka. This means SimpleConsumers can be sped up to handle larger
streams without rebalancing, and it also means BalancedConsumer's get
better per-core or per-process utilization. Making use of this protocol
is as simple as passing a ``use_rdkafka=True`` flag to the appropriate
consumer or producer creation functions.

Compatibility Matrix
--------------------

Kafka lacks a coherent release management process, which is one of the
worst parts of the project. Minor dot-version releases have dramatically
changed client protocols, thus resembling major version changes to
client teams working on projects like pykafka. To help sort through the
noise, here is a compatibility matrix for Kafka versions of whether we
have protocol support for these versions in latest stable versions of
our consumer/producer classes:

+-----------------+------------+------------+
| Kafka version   | pykafka?   | rdkafka?   |
+=================+============+============+
| 0.8.1           | No         | No         |
+-----------------+------------+------------+
| 0.8.2           | Yes        | Yes        |
+-----------------+------------+------------+
| 0.9.0           | Planned    | Planned    |
+-----------------+------------+------------+

Note that 0.9.0.0 is currently in "release candidate" stage as of
November 2015.

Core Kafka Issues On Our Radar
------------------------------

There are several important Kafka core issues that are on our radar and
that have changed things dramatically (hopefully for the better) in the
new Kafka 0.9.0 release version. These are summarized in this table:

+---------------------------+-------------+--------------+---------------------------------------------------------------------+
| Issue                     | 0.8.2       | 0.9.0        | Link?                                                               |
+===========================+=============+==============+=====================================================================+
| New Consumer API          | N/A         | Added        | `KAFKA-1328 <https://issues.apache.org/jira/browse/KAFKA-1328>`__   |
+---------------------------+-------------+--------------+---------------------------------------------------------------------+
| New Consumer API Extras   | N/A         | In Flux      | `KAFKA-1326 <https://issues.apache.org/jira/browse/KAFKA-1326>`__   |
+---------------------------+-------------+--------------+---------------------------------------------------------------------+
| Security/SSL              | N/A         | Added        | `KAFKA-1682 <https://issues.apache.org/jira/browse/KAFKA-1682>`__   |
+---------------------------+-------------+--------------+---------------------------------------------------------------------+
| Broker/ZK Crash           | Bug         | Fixed        | `KAFKA-1387 <https://issues.apache.org/jira/browse/KAFKA-1387>`__   |
+---------------------------+-------------+--------------+---------------------------------------------------------------------+
| Documentation             | "Minimal"   | "Improved"   | `New Docs <http://kafka.apache.org/090/documentation.html>`__       |
+---------------------------+-------------+--------------+---------------------------------------------------------------------+

Let's focus on three areas here: new consumer API, security, and
broker/ZK crash.

New Consumer API
~~~~~~~~~~~~~~~~

One of the biggest new features of Kafka 0.9.0 is a brand new Consumer
API. The good news **may** be that despite introducing this new API,
they **may** still support their "old" APIs that were stabilized in
Kafka 0.8.2. We are going to explore this as this would provide a
smoother upgrade path for pykafka users for certain.

The main difference for this new API is moving more of the
BalancedConsumer partition rebalancing logic into the broker itself.
This would certainly be a good idea to standardize how BalancedConsumers
work across programming languages, but we don't have a lot of confidence
that this protocol is bug-free at the moment. The Kafka team even
describes **their own** 0.9.0 consumer as being "beta quality".

Security/SSL
~~~~~~~~~~~~

This is one of Kafka's top requests. To provide secure access to Kafka
topics, people have had to use the typical IP whitelisting and VPN
hacks, which is problematic since they can often impact the overall
security of a system, impact performance, and are operationally complex
to maintain.

The Kafka 0.9.0 release includes a standard mechanism for doing
SSL-based security in communicating with Kafka brokers. We'll need to
explore what the requirements and limitations are of this scheme to see
if it can be supported directly by pykafka.

Broker/ZK Crash
~~~~~~~~~~~~~~~

This is perhaps the most annoying issue regarding this new release. We
have several reports from the community of Kafka brokers that crash as a
result of a coordination issue with Zookeeper. A bug fix was worked on
for several months and a patched build of 0.8.1 fixed the issue
permanently for some users, but because the Kafka community cancelled a
0.8.3 release, favoring 0.9.0 instead, no patched build of 0.8.2 was
ever created. This issue **is** fixed in 0.9.0, however.

The Way Forward
---------------

We want pykafka to support 0.8.2 and 0.9.0 in a single source tree. We'd
like the rdkafka implementation to have similar support. We think this
will likely be supported **without** using Kafka's 0.9.0 "New Consumer
API". This will give users a 0.9.0 upgrade path for stability (fixing
the Broker/ZK Crash, and allowing use of SimpleConsumer,
BalancedConsumer, and C-optimized versions with rdkafka).

We don't know, yet, whether the new Security/SSL scheme requires use of
the new Consumer APIs. If so, the latter may be a blocker for the
former. We will likely discover the answer to this in November 2015.

A `tracker issue for Kafka 0.9.0
support <https://github.com/Parsely/pykafka/issues/349>`__ in pykafka
was opened, and that's where discussion should go for now.
