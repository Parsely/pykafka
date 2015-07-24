import json
import random
import subprocess
import timeit

from pykafka import KafkaClient
from pykafka.test.utils import get_cluster


NUM_PARTITIONS_RANGE = [4, 8, 16, 32, 64]
MSG_SIZE_BYTES_RANGE = [100, 1000, 10**4, 10**5]


def get_topic_name(num_partitions, msg_size_bytes):
    """Generate test-topic names describing message characteristics"""
    return "parts{}-size{}".format(num_partitions, msg_size_bytes)


def mk_topics(num_partitions_range=NUM_PARTITIONS_RANGE,
              msg_size_bytes_range=MSG_SIZE_BYTES_RANGE,
              msg_sum_bytes=10**9,
              max_num_msgs=10**6):
    """
    Pre-populate an existing testinstances cluster with test messages

    A topic is created for each combination of partition-count and
    message-size (passed in iterables).  Note that it's easy to eat lots of
    disk space with this!

    :param msg_sum_bytes: Limit the number of messages written to a topic
                          so that topic will not exceed this number of bytes
    :param max_num_msgs: Limit to number of messages written to any topic
    """
    cluster = get_cluster()  # make this in advance, like in travis.yml
    topics = [(np, sz, get_topic_name(np, sz))
              for np in num_partitions_range
              for sz in msg_size_bytes_range]

    for num_partitions, msg_size_bytes, topic_name in topics:
        cluster.create_topic(topic_name, num_partitions, replication_factor=1)
        print "Producing into {}".format(topic_name)
        client = KafkaClient(cluster.brokers)
        n_msgs = min(msg_sum_bytes // msg_size_bytes, max_num_msgs)
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


def run_bench(consumer_type,
              num_partitions,
              msg_size_bytes,
              num_consumer_fetchers,
              queued_max_messages,
              num_iterations,
              filename_append="consumer_bench.json"):
    """
    Run a single timeit-benchmark for num_iterations of consume() calls

    This is a somewhat awkward-looking runner, because we currently have an
    issue where consumers don't get garbage-collected, and so their worker
    threads keep going even after timeit is done with them (and even if we do
    slip in a stop() call, they don't release the memory for their message
    queues - I tried that).  As a workaround, we must exit the interpreter
    after every call to run_bench (cf run_bench_in_shell), and then aggregate
    the running times in the analysis script instead.

    :param consumer_type: Either "pure-py" or "rdkafka"
    :param num_partitions: Number of partitions in topic. You need to prepare
                           a test-topic with this parameter beforehand, see
                           mk_topics()
    :param msg_size_bytes: Size of messages in topic. You need to prepare
                           a test-topic with this parameter beforehand, see
                           mk_topics()
    :param num_consumer_fetchers: Passed to consumer init
    :param queued_max_messages: Passed to consumer init
    :param num_iterations: Number of iterations in timeit
    :param filename_append: A line of JSONified benchmark-data is appended to
                            the specified file
    """
    setup = (SETUP.format(topic_name=get_topic_name(num_partitions,
                                                    msg_size_bytes),
                          num_consumer_fetchers=num_consumer_fetchers,
                          queued_max_messages=queued_max_messages)
             + SETUP_FIN[consumer_type])

    timer = timeit.Timer("cons.consume().value", setup)
    runtime_secs = timer.timeit(num_iterations)

    data = {k: v for k, v in vars().items() if k in ("consumer_type",
                                                     "num_partitions",
                                                     "msg_size_bytes",
                                                     "num_consumer_fetchers",
                                                     "queued_max_messages",
                                                     "num_iterations",
                                                     "runtime_secs")}
    data["num_brokers"] = len(get_cluster().brokers.split(','))
    with open(filename_append, 'a') as f:
        f.write(json.dumps(data) + '\n')
    print data


def run_bench_in_shell():
    """
    Sample parameter space randomly, calling run_bench through a shell

    (See run_bench for why this needs to be wrapped in a separate
    interpreter.)  This is all very haphazard, originally just written to
    use once and throw away.  It runs benchmarks jumping around over topic
    configurations and consumer-init parameters (see function body, sorry).
    Using the accompanying analysis script, you can then visualize data
    points while this keeps spitting out new ones, and maybe identify
    interesting areas - to then stop this and reconfigure it to zoom in on
    those areas.
    """
    while True:
        # Values we choose here must have been fed to mk_topics() before
        num_partitions = random.choice(NUM_PARTITIONS_RANGE)
        msg_size_bytes = random.choice(MSG_SIZE_BYTES_RANGE)

        # currently testing against an 8-node cluster, so max of 4 fetchers:
        num_consumer_fetchers = random.choice([1, 2, 4])

        queued_max_messages = random.choice([2000, 10000, 20000, 10**5])
        num_iterations = min(10**9 // msg_size_bytes, 10**5)

        seq = 3 * ["pure-py", "rdkafka"]
        # At least one of these runs may be impacted by a cold disk cache
        # (we're jumping between GB-size topics), so we'll shuffle a bit.  I
        # haven't thought about it too hard because we'll typically regard only
        # the fastest run time anyway:
        random.shuffle(seq)
        for consumer_type in seq:
            exec_str = (
                "from pykafka.rdkafka.simple_consumer_bench import run_bench; "
                "run_bench('{consumer_type}', {num_partitions}, "
                "{msg_size_bytes}, {num_consumer_fetchers}, "
                "{queued_max_messages}, {num_iterations})".format(**vars()))
            subprocess.call('python -c "{}"'.format(exec_str), shell=True)
