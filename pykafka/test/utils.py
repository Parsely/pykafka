import time
import os

from pykafka.test.kafka_instance import KafkaInstance, KafkaConnection


def get_cluster():
    """Gets a Kafka cluster for testing, using one already running is possible.

    An already-running cluster is determined by environment variables:
    BROKERS, ZOOKEEPER, KAFKA_BIN.  This is used primarily to speed up tests
    in our Travis-CI environment.
    """
    if os.environ.get('BROKERS', None) and \
       os.environ.get('ZOOKEEPER', None) and \
       os.environ.get('KAFKA_BIN', None):
        # Broker is already running. Use that.
        return KafkaConnection(os.environ['KAFKA_BIN'],
                               os.environ['BROKERS'],
                               os.environ['ZOOKEEPER'],
                               os.environ.get('BROKERS_SSL', None))
    else:
        return KafkaInstance(num_instances=3)


def stop_cluster(cluster):
    """Stop a created cluster, or merely flush a pre-existing one."""
    if isinstance(cluster, KafkaInstance):
        cluster.terminate()
    else:
        cluster.flush()


def retry(assertion_callable, retry_time=10, wait_between_tries=0.1, exception_to_retry=AssertionError):
    """Retry assertion callable in a loop"""
    start = time.time()
    while True:
        try:
            return assertion_callable()
        except exception_to_retry as e:
            if time.time() - start >= retry_time:
                raise e
            time.sleep(wait_between_tries)
