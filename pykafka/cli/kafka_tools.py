import argparse
import calendar
import datetime as dt

import tabulate

import pykafka
from pykafka.common import OffsetType

#
# Helper Functions
#


def fetch_offsets(client, topic, offset):
    """Fetch raw offset data from a topic.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`pykafka.topic.Topic`
    :param offset: Offset to reset to. Can be earliest, latest or a datetime.
        Using a datetime will reset the offset to the latest message published
        *before* the datetime.
    :type offset: :class:`pykafka.common.OffsetType` or
        :class:`datetime.datetime`
    :returns: dict of {partition_id: :class:`pykafka.protocol.OffsetPartitionResponse`}
    """
    if offset.lower() == 'earliest':
        return topic.earliest_available_offsets()
    elif offset.lower() == 'latest':
        return topic.latest_available_offsets()
    else:
        offset = dt.datetime.strptime(offset, "%Y-%m-%dT%H:%M:%S")
        offset = int(calendar.timegm(offset.utctimetuple())*1000)
        return topic.fetch_offset_limits(offset)


def fetch_consumer_lag(client, topic, consumer_group):
    """Get raw lag data for a topic/consumer group.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`pykafka.topic.Topic`
    :param consumer_group: Name of the consumer group to reset offsets for.
    :type consumer_groups: :class:`str`
    :returns: dict of {partition_id: (latest_offset, consumer_offset)}
    """
    latest_offsets = fetch_offsets(client, topic, 'latest')
    consumer = topic.get_simple_consumer(consumer_group=consumer_group,
                                         auto_start=False)
    current_offsets = consumer.fetch_offsets()
    return {p_id: (latest_offsets[p_id].offset[0], res.offset)
            for p_id, res in current_offsets}


#
# Commands
#

def desc_topic(client, topic):
    """Print detailed information about a topic.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`str`
    """
    # Don't auto-create topics.
    if topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(topic))
    topic = client.topics[topic]
    print 'Topic: {}'.format(topic.name)
    print 'Partitions: {}'.format(len(topic.partitions))
    print 'Replicas: {}'.format(len(topic.partitions.values()[0].replicas))
    print tabulate.tabulate(
        [(p.id, p.leader.id, [r.id for r in p.replicas], [r.id for r in p.isr])
         for p in topic.partitions.values()],
        headers=['Partition', 'Leader', 'Replicas', 'ISR'],
        numalign='center',
    )


def print_consumer_lag(client, topic, consumer_group):
    """Print lag for a topic/consumer group.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`str`
    :param consumer_group: Name of the consumer group to reset offsets for.
    :type consumer_groups: :class:`str`
    """
    # Don't auto-create topics.
    if topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(topic))
    topic = client.topics[topic]

    lag_info = fetch_consumer_lag(client, topic, consumer_group)
    lag_info = [(k, '{:,}'.format(v[0] - v[1]), v[0], v[1])
                for k,v in lag_info.iteritems()]
    print tabulate.tabulate(
        lag_info,
        headers=['Partition', 'Lag', 'Latest Offset', 'Current Offset'],
        numalign='center',
    )

    total = sum(int(i[1].replace(',', '')) for i in lag_info)
    print '\n Total lag: {:,} messages.'.format(total)


def print_offsets(client, topic, offset):
    """Print offsets for a topic/consumer group.

    NOTE: Time-based offset lookups are not precise, but are based on segment
          boundaries. If there is only one segment, as when Kafka has just
          started, the only offsets found will be [0, <latest_offset>].

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`str`
    :param offset: Offset to reset to. Can be earliest, latest or a datetime.
        Using a datetime will reset the offset to the latest message published
        *before* the datetime.
    :type offset: :class:`pykafka.common.OffsetType` or
        :class:`datetime.datetime`
    """
    # Don't auto-create topics.
    if topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(topic))
    topic = client.topics[topic]

    offsets = fetch_offsets(client, topic, offset)
    print tabulate.tabulate(
        [(k, v.offset[0]) for k,v in offsets.iteritems()],
        headers=['Partition', 'Offset'],
        numalign='center',
    )


def print_topics(client):
    """Print all topics in the cluster.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    """
    print tabulate.tabulate(
        [(t.name,
          len(t.partitions),
          len(t.partitions.values()[0].replicas) - 1)
         for t in client.topics.values()],
        headers=['Topic', 'Partitions', 'Replication'],
        numalign='center',
    )


def reset_offsets(client, topic, consumer_group, offset):
    """Reset offset for a topic/consumer group.

    NOTE: Time-based offset lookups are not precise, but are based on segment
          boundaries. If there is only one segment, as when Kafka has just
          started, the only offsets found will be [0, <latest_offset>].

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`str`
    :param consumer_group: Name of the consumer group to reset offsets for.
    :type consumer_groups: :class:`str`
    :param offset: Offset to reset to. Can be earliest, latest or a datetime.
        Using a datetime will reset the offset to the latest message published
        *before* the datetime.
    :type offset: :class:`pykafka.common.OffsetType` or
        :class:`datetime.datetime`
    """
    # Don't auto-create topics.
    if topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(topic))
    topic = client.topics[topic]

    offsets = fetch_offsets(client, topic, offset)
    consumer = topic.get_simple_consumer(consumer_group=consumer_group,
                                         auto_start=False)
    for owned_partition, partition in consumer._partitions.iteritems():
        owned_partition.last_offset_consumed = int(offsets[partition.id].offset[0])
    consumer.commit_offsets()


def _get_arg_parser():
    output = argparse.ArgumentParser(description='Tools for Kafka.')

    # Common arguments
    output.add_argument('-b', '--broker',
                        required=False,
                        default='localhost:9092',
                        dest='host',
                        help='host:port of any Kafka broker. '
                             '[default: localhost:9092]')

    subparsers = output.add_subparsers(help='Commands', dest='command')

    # Desc Topic
    parser = subparsers.add_parser(
        'desc_topic',
        help='Print detailed info for a topic.'
    )
    parser.add_argument('topic',
                        metavar='TOPIC',
                        help='Topic name.')

    # Print Consumer Lag
    parser = subparsers.add_parser(
        'print_consumer_lag',
        help='Get consumer lag for a topic.'
    )
    parser.add_argument('topic',
                        metavar='TOPIC',
                        help='Topic name.')
    parser.add_argument('consumer_group',
                        metavar='CONUSMER_GROUP',
                        help='Consumer group name.')

    # Print Offsets
    parser = subparsers.add_parser(
        'print_offsets',
        help='Fetch offsets for a topic/consumer group'
    )
    parser.add_argument('topic',
                        metavar='TOPIC',
                        help='Topic name.')
    parser.add_argument('offset',
                        metavar='OFFSET',
                        type=str,
                        help='Offset to fetch. Can be EARLIEST, LATEST, or a '
                             'datetime in the format YYYY-MM-DDTHH:MM:SS.')

    # Print Topics
    parser = subparsers.add_parser(
        'print_topics',
        help='Print information about all topics in the cluster.'
    )


    # Reset Offsets
    parser = subparsers.add_parser(
        'reset_offsets',
        help='Reset offsets for a topic/consumer group'
    )
    parser.add_argument('topic',
                        metavar='TOPIC',
                        help='Topic to reset offsets for.')
    parser.add_argument('consumer_group',
                        metavar='CONUSMER_GROUP',
                        help='Consumer group to reset offsets for.')
    parser.add_argument('offset',
                        metavar='OFFSET',
                        type=str,
                        help='Target offset. Can be EARLIEST, LATEST, or a '
                             'datetime in the format YYYY-MM-DDTHH:MM:SS.')

    return output


def main():
    parser = _get_arg_parser()
    args = parser.parse_args()

    # Connect to Kafka
    client = pykafka.KafkaClient(hosts=args.host)

    if args.command == 'reset_offsets':
        reset_offsets(client, args.topic, args.consumer_group, args.offset)
    elif args.command == 'print_offsets':
        print_offsets(client, args.topic, args.offset)
    elif args.command == 'print_consumer_lag':
        print_consumer_lag(client, args.topic, args.consumer_group)
    elif args.command == 'print_topics':
        print_topics(client)
    elif args.command == 'desc_topic':
        desc_topic(client, args.topic)

if __name__ == '__main__':
    main()
