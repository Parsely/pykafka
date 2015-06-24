import json
import timeit
import uuid

from . import RdKafkaSimpleConsumer
from pykafka import KafkaClient
from pykafka.test.utils import get_cluster


NUM_PARTITIONS_RANGE = [4, 8, 16, 32, 64]
MSG_SIZE_BYTES_RANGE = [100, 1000, 10**4, 10**5]


def get_topic_name(num_partitions, msg_size_bytes):
    return "parts{}-size{}".format(num_partitions, msg_size_bytes)


def mk_topics(num_partitions_range=NUM_PARTITIONS_RANGE,
              msg_size_bytes_range=MSG_SIZE_BYTES_RANGE,
              msg_sum_bytes = 10**9):
    cluster = get_cluster()  # make this in advance, like in travis.yml
    topics = [(np, sz, get_topic_name(np, sz))
              for np in num_partitions_range
              for sz in msg_size_bytes_range]

    for num_partitions, msg_size_bytes, topic_name in topics:
        cluster.create_topic(topic_name, num_partitions, replication_factor=1)
        print "Producing into {}".format(topic_name)
        client = KafkaClient(cluster.brokers)
        n_msgs = msg_sum_bytes // msg_size_bytes
        prod = client.topics[topic_name].get_producer()
        prod.produce(msg_size_bytes * b" " for _ in xrange(n_msgs))


SETUP = ("from pykafka import KafkaClient\n"
         "from pykafka.rdkafka import RdKafkaSimpleConsumer\n"
         "from pykafka.common import OffsetType\n"
         "client = KafkaClient('localhost:9092')\n"
         "topic = client.topics['{topic_name}']\n"
         "kwargs = dict(\n"
         "    auto_offset_reset=OffsetType.EARLIEST,\n"
         "    consumer_timeout_ms=5000,\n"  # with default (-1) it might hang
         "    num_consumer_fetchers={num_consumer_fetchers},\n"
         "    queued_max_messages={queued_max_messages},\n"
         "    )\n")
SETUP_FIN = {
    "pure-py": "cons = topic.get_simple_consumer(**kwargs)",
    "rdkafka": "cons = RdKafkaSimpleConsumer(topic, client.cluster, **kwargs)",
    }

# We seem to have an issue where consumers don't get garbage-collected, and so
# their worker threads keep going even after timeit is done with them.  As a
# workaround, we loop manually, and call stop() inside the timing run.  That
# shouldn't affect results much if we loop long enough:
# FIXME except... it does actually affect results.  Also, the stopped
# consumers still don't get gc'd so memory use is bizarre after a few runs.
# Switch to single repeats and calling from __main__.
LOOP = ("count = 0\n"
        "try:\n"
        "    for _ in xrange({loop}):\n"
        "        count += cons.consume().offset\n"
        "finally:\n"
        "    cons.stop()\n"
        "print 'loop finished'\n")


def run_bench(consumer_type,  # "pure-py" or "rdkafka"
              num_partitions,  # this and the next must form ...
              msg_size_bytes,  # ... a combo previously created by mk_topics
              num_consumer_fetchers,
              queued_max_messages,
              loop,  # don't exceed n_msgs created in mk_topics()
              repeat=3,
              filename_append="consumer_bench.json"):
    setup = (SETUP.format(topic_name=get_topic_name(num_partitions,
                                                    msg_size_bytes),
                          num_consumer_fetchers=num_consumer_fetchers,
                          queued_max_messages=queued_max_messages)
             + SETUP_FIN[consumer_type])

    timer = timeit.Timer(LOOP.format(loop=loop), setup)
    results = timer.repeat(repeat, 1)

    data = {
        "consumer_type": consumer_type,
        "num_brokers": len(get_cluster().brokers.split(',')),
        "num_partitions": num_partitions,
        "msg_size_bytes": msg_size_bytes,
        "num_consumer_fetchers": num_consumer_fetchers,
        "queued_max_messages": queued_max_messages,
        "num_iterations": loop,
        "results_secs": results
        }
    with open(filename_append, 'a') as f:
        f.write(json.dumps(data) + '\n')
