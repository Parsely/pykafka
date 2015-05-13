import os

from pykafka.test.kafka_instance import KafkaInstance, KafkaConnection


def get_cluster():
    """Gets a Kafka cluster for testing, using one already running is possible.

    An already-running cluster is determined by environment variables:
    BROKERS, ZOOKEEPER, KAFKA_BIN.  This is used primarily to speed up tests
    in our Travis-CI environment.
    """
    if 'BROKERS' in os.environ and \
       'ZOOKEEPER' in os.environ and \
       'KAFKA_BIN' in os.environ:
        # Broker is already running. Use that.
        return KafkaConnection(os.environ['KAFKA_BIN'],
                               os.environ['BROKERS'],
                               os.environ['ZOOKEEPER'])
    else:
        return KafkaInstance(num_instances=3)


def stop_cluster(cluster):
    """Stop a created cluster, or merely flush a pre-existing one."""
    if isinstance(cluster, KafkaInstance):
        cluster.terminate()
    else:
        cluster.flush()
