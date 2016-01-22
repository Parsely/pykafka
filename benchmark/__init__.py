import cPickle as pickle
import itertools
import multiprocessing
import resource
import time
import traceback
from uuid import uuid4

from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.test.utils import KafkaConnection


NUM_PARTITIONS_RANGE = [4, 8, 16, 32, 64]
MSG_SIZE_BYTES_RANGE = [100, 1000, 10**4, 10**5]
MAX_DISK_BYTES = 10**9  # cap disk footprint of test topics
MAX_NUM_ITERATIONS = 10**6  # cap on messages per benchmark round
NUM_ROUNDS = 3  # number of rounds to repeat with same parameters

CONSUMER_FETCHERS_RANGE = [1, 2, 4]
CONSUMER_QUEUE_SIZES = [2000, 10000, 20000, 10**5]


def pick_num_iterations(msg_size_bytes):
    """Decide number of loop iterations in a benchmark round

    Don't have too many (because it would either take forever, or we'd exceed
    OS-level disk cache, which might distort numbers), but also don't have
    too few messages (because overhead would dominate)
    """
    # This may have to be tuned to the test platform
    return min(MAX_DISK_BYTES // msg_size_bytes, MAX_NUM_ITERATIONS)


def run_benchmarks(runner, file_path):
    """Run benchmarks and append pickled results to file

    :param runner: Function, either benchmark_consumer or benchmark_producer
    """
    pkl_file = open(file_path, 'ab')

    # Assume an up and running KafkaInstance:
    kafka_conn = KafkaConnection("/tmp/kafka-bin",
                                 "localhost:9092",
                                 "localhost:2181")

    num_brokers = count_brokers(kafka_conn)
    for num_partitions, msg_size_bytes in itertools.product(
            NUM_PARTITIONS_RANGE, MSG_SIZE_BYTES_RANGE):
        num_iterations = pick_num_iterations(msg_size_bytes)

        base_params =  dict(num_iterations=num_iterations,
                            num_brokers=num_brokers,
                            num_partitions=num_partitions,
                            msg_size_bytes=msg_size_bytes)
        for params, output in runner(
                kafka_conn, num_partitions, num_iterations, msg_size_bytes):
            params.update(base_params)
            append_to_pkl(params, output, pkl_file)


def append_to_pkl(params, output, pkl_file):
    pickle.dump(dict(output=output, params=params), pkl_file, protocol=2)
    pkl_file.flush()  # just so I can impatiently tail this file


def benchmark_producer(kafka_conn,
                       num_partitions,
                       num_iterations,
                       msg_size_bytes):
    """Benchmark Producer, yielding (parameters, results) tuples"""
    parent_conn, child_conn = multiprocessing.Pipe()
    for use_rdkafka, rnd in itertools.product((True, False), range(NUM_ROUNDS)):
        init_kwargs = dict(use_rdkafka=use_rdkafka, linger_ms=100)

        # Write a few messages just to make sure topic is ready to go
        topic_name = mk_topic(kafka_conn,
                              num_partitions,
                              msg_size_bytes=msg_size_bytes,
                              max_num_msgs=10)
        try:
            proc = multiprocessing.Process(
                    target=benchmark_producer_subp,
                    args=(child_conn,
                          kafka_conn.brokers,
                          topic_name,
                          num_iterations,
                          msg_size_bytes),
                    kwargs=init_kwargs)
            proc.start()
            proc.join()
            assert proc.exitcode == 0
            output = parent_conn.recv()
        except:
            output = dict(traceback=traceback.format_exc())
        finally:
            kafka_conn.delete_topic(topic_name)
        yield init_kwargs, output


def benchmark_producer_subp(connection,
                            broker_string,
                            topic_name,
                            num_iterations,
                            msg_size_bytes,
                            **init_kwargs):
    """Run producer loop for benchmarking

    This is meant to be run in a `multiprocessing.Process`, so that data
    collected through the `rusage` module only applies to this single
    benchmark run.  Results are relayed back through a `multiprocessing.Pipe`

    :param connection: One end of a `multiprocessing.Pipe`, to which results
        shall be sent before exiting
    """
    try:
        client = KafkaClient(broker_string)
        topic = client.topics[topic_name]
        with topic.get_producer(**init_kwargs) as producer:
            time.sleep(1.)  # allow producer workers time to "settle"
            t_begin = time.time()
            for _ in xrange(num_iterations):
                producer.produce(msg_size_bytes * b" ")
        # Record the end time outside the contextmanager, to include the time for
        # which it blocks flushing the queue on exit
        t_end = time.time()
        rusage = resource.getrusage(resource.RUSAGE_SELF)
    except:
        output = dict(traceback=traceback.format_exc())
    else:
        output = dict(t_begin=t_begin, t_end=t_end, rusage=rusage)
    connection.send(output)


def benchmark_consumer(kafka_conn,
                       num_partitions,
                       num_iterations,
                       msg_size_bytes):
    """Benchmark Consumer, yielding (parameters, results) tuples"""
    topic_name = mk_topic(kafka_conn,
                          num_partitions,
                          msg_size_bytes=msg_size_bytes)

    # Warm up the disk cache (possibly superfluous since we just wrote topic)
    basic_kwargs = dict(
        auto_offset_reset=OffsetType.EARLIEST,
        consumer_timeout_ms=5000)  # with default (-1) it might hang
    parent_conn, child_conn = multiprocessing.Pipe()
    proc = multiprocessing.Process(
            target=benchmark_consumer_subp,
            args=(child_conn,
                  kafka_conn.brokers,
                  topic_name,
                  num_iterations),
            kwargs=basic_kwargs)
    proc.start()
    proc.join()
    parent_conn.recv()  # discard output

    try:
        for use_rdkafka, queued_max_messages in itertools.product(
                (True, False), CONSUMER_QUEUE_SIZES):
            fetchers = [1] if use_rdkafka else CONSUMER_FETCHERS_RANGE
            for num_consumer_fetchers, rnd in itertools.product(
                    fetchers, range(NUM_ROUNDS)):
                init_kwargs = dict(
                    use_rdkafka=use_rdkafka,
                    queued_max_messages=queued_max_messages,
                    num_consumer_fetchers=num_consumer_fetchers,
                    **basic_kwargs)
                try:
                    proc = multiprocessing.Process(
                            target=benchmark_consumer_subp,
                            args=(child_conn,
                                  kafka_conn.brokers,
                                  topic_name,
                                  num_iterations),
                            kwargs=init_kwargs)
                    proc.start()
                    proc.join()
                    assert proc.exitcode == 0
                    output = parent_conn.recv()
                except:
                    output = dict(traceback=traceback.format_exc())
                yield init_kwargs, output
    finally:
        kafka_conn.delete_topic(topic_name)


def benchmark_consumer_subp(connection,
                            broker_string,
                            topic_name,
                            num_iterations,
                            **init_kwargs):
    try:
        client = KafkaClient(broker_string)
        topic = client.topics[topic_name]

        # We include the instantiation of the consumer in the timed run,
        # because we'd otherwise see large timing variations depending on the
        # size of the prefetch queue, and whether that queue was full up when
        # the timer started
        t_begin = time.time()
        consumer = topic.get_simple_consumer(**init_kwargs)
        for _ in xrange(num_iterations):
            assert consumer.consume().value is not None
        t_end = time.time()
        rusage = resource.getrusage(resource.RUSAGE_SELF)
    except:
        output = dict(traceback=traceback.format_exc())
    else:
        output = dict(t_begin=t_begin, t_end=t_end, rusage=rusage)
    connection.send(output)


def mk_topic(kafka_conn,
             num_partitions,
             replication_factor=1,
             msg_size_bytes=None,
             msg_sum_bytes=int(1.1 * MAX_DISK_BYTES),
             max_num_msgs=int(1.1 * MAX_NUM_ITERATIONS)):
    """
    Prepare a test-topic, optionally prefilled with messages, return its name

    :param msg_size_bytes: If given, prefill topic with messages of this size
    :param msg_sum_bytes: Limit the number of messages written to a topic
                          so that topic will not exceed this number of bytes
    :param max_num_msgs: Limit to number of messages written
    """
    topic_name = uuid4().hex.encode()
    kafka_conn.create_topic(topic_name, num_partitions, replication_factor)

    client = KafkaClient(kafka_conn.brokers)
    topic = client.topics[topic_name]
    if msg_size_bytes is not None:
        n_msgs = min(msg_sum_bytes // msg_size_bytes, max_num_msgs)
        with topic.get_producer() as prod:
            for _ in xrange(n_msgs):
                prod.produce(msg_size_bytes * b" ")
    return topic_name


def count_brokers(kafka_conn):
    """Blunt way to obtain the number of brokers in the cluster"""
    client = KafkaClient(kafka_conn.brokers)
    return len(client.brokers)
