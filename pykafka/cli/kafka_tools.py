from __future__ import print_function

import argparse
import calendar
import datetime as dt
import sys
import time
from pkg_resources import parse_version

import tabulate

import pykafka
from pykafka.common import OffsetType
from pykafka.protocol import PartitionOffsetCommitRequest, CreateTopicRequest
from pykafka.utils.compat import PY3, iteritems

#
# Helper Functions
#


def fetch_offsets(client, topic, offset):
    """Fetch raw offset data from a topic.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`pykafka.topic.Topic`
    :param offset: Offset to fetch. Can be earliest, latest or a datetime.
    :type offset: :class:`pykafka.common.OffsetType` or
        :class:`datetime.datetime`
    :returns: {partition_id: :class:`pykafka.protocol.OffsetPartitionResponse`}
    """
    if offset.lower() == 'earliest':
        return topic.earliest_available_offsets()
    elif offset.lower() == 'latest':
        return topic.latest_available_offsets()
    else:
        offset = dt.datetime.strptime(offset, "%Y-%m-%dT%H:%M:%S")
        offset = int(calendar.timegm(offset.utctimetuple()) * 1000)
        return topic.fetch_offset_limits(offset)


def fetch_consumer_lag(client, topic, consumer_group):
    """Get raw lag data for a topic/consumer group.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`pykafka.topic.Topic`
    :param consumer_group: Name of the consumer group to fetch lag for.
    :type consumer_groups: :class:`str`
    :returns: dict of {partition_id: (latest_offset, consumer_offset)}
    """
    latest_offsets = fetch_offsets(client, topic, 'latest')
    consumer = topic.get_simple_consumer(consumer_group=consumer_group,
                                         auto_start=False, 
                                         reset_offset_on_fetch=False)
    current_offsets = consumer.fetch_offsets()
    return {p_id: (latest_offsets[p_id].offset[0], res.offset)
            for p_id, res in current_offsets}


#
# Commands
#

def consume_topic(client, args):
    """Dump messages from a topic to a file or stdout.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`str`
    """
    # Don't auto-create topics.
    if args.topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(args.topic))
    topic = client.topics[args.topic]
    consumer = topic.get_simple_consumer("pykafka_cli",
                                         consumer_timeout_ms=100,
                                         auto_offset_reset=OffsetType.LATEST,
                                         reset_offset_on_start=True,
                                         auto_commit_enable=True,
                                         auto_commit_interval_ms=3000)
    num_consumed = 0
    while num_consumed < args.limit:
        msg = consumer.consume()
        if not msg or not msg.value:
            continue
        # Coming from Kafka, this is utf-8.
        msg = msg.value.decode('utf-8', errors='replace') + u'\n'
        args.outfile.write(msg if PY3 else msg.encode('utf-8'))
        num_consumed += 1
    args.outfile.close()


def desc_topic(client, args):
    """Print detailed information about a topic.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`str`
    """
    # Don't auto-create topics.
    if args.topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(args.topic))
    topic = client.topics[args.topic]
    print('Topic: {}'.format(topic.name))
    print('Partitions: {}'.format(len(topic.partitions)))
    print('Replicas: {}'.format(len(list(topic.partitions.values())[0].replicas)))
    print(tabulate.tabulate(
        [(p.id, p.leader.id, [r.id for r in p.replicas], [r.id for r in p.isr])
         for p in topic.partitions.values()],
        headers=['Partition', 'Leader', 'Replicas', 'ISR'],
        numalign='center',
    ))


def print_managed_consumer_groups(client, args):
    """Get Kafka-managed consumer groups for a topic.

        :param client: KafkaClient connected to the cluster.
        :type client:  :class:`pykafka.KafkaClient`
        :param topic:  Name of the topic.
        :type topic:  :class:`str`
    """
    if args.topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(args.topic))
    consumer_groups = {}
    brokers = client.brokers
    for broker_id, broker in iteritems(brokers):
        groups = broker.list_groups().groups.keys()
        groups_metadata = broker.describe_groups(group_ids=groups).groups
        for group_id, describe_group_response in iteritems(groups_metadata):
            members = describe_group_response.members
            for member_id, member in iteritems(members):
                topics = member.member_metadata.topic_names
                if args.topic in topics:
                    consumer_groups[group_id] = describe_group_response

    print('Topic: {}'.format(args.topic))
    print(tabulate.tabulate(
        [(group_id, x.state, x.protocol, x.protocol_type)
         for group_id, x in iteritems(consumer_groups)],
        headers=['GroupId', 'State', 'Protocol', 'ProtocolType']
    ))


def print_consumer_lag(client, args):
    """Print lag for a topic/consumer group.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`str`
    :param consumer_group: Name of the consumer group to fetch offsets for.
    :type consumer_groups: :class:`str`
    """
    # Don't auto-create topics.
    if args.topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(args.topic))
    topic = client.topics[args.topic]

    lag_info = fetch_consumer_lag(client, topic, args.consumer_group)
    lag_info = [(k, '{:,}'.format(v[0] - v[1]), v[0], v[1])
                for k, v in iteritems(lag_info)]
    print(tabulate.tabulate(
        lag_info,
        headers=['Partition', 'Lag', 'Latest Offset', 'Current Offset'],
        numalign='center',
    ))

    total = sum(int(i[1].replace(',', '')) for i in lag_info)
    print('\n Total lag: {:,} messages.'.format(total))


def print_offsets(client, args):
    """Print offsets for a topic

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
    if args.topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(args.topic))
    topic = client.topics[args.topic]

    offsets = fetch_offsets(client, topic, args.offset)
    print(tabulate.tabulate(
        [(k, v.offset[0]) for k, v in iteritems(offsets)],
        headers=['Partition', 'Offset'],
        numalign='center',
    ))


def print_topics(client, args):
    """Print all topics in the cluster.

    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    """
    print(tabulate.tabulate(
        [(t.name,
          len(t.partitions),
          len(list(t.partitions.values())[0].replicas) - 1)
         for t in client.topics.values()],
        headers=['Topic', 'Partitions', 'Replication'],
        numalign='center',
    ))


def reset_offsets(client, args):
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
    if args.topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(args.topic))
    topic = client.topics[args.topic]

    # Build offset commit requests.
    offsets = fetch_offsets(client, topic, args.offset)
    tmsp = int(time.time() * 1000)
    reqs = [PartitionOffsetCommitRequest(topic.name,
                                         partition_id,
                                         res.offset[0],
                                         tmsp,
                                         b'kafka-tools')
            for partition_id, res in iteritems(offsets)]

    # Send them to the appropriate broker.
    broker = client.cluster.get_group_coordinator(args.consumer_group)
    broker.commit_consumer_group_offsets(
        args.consumer_group, 1, b'kafka-tools', reqs
    )


def create_topic(client, args):
    if parse_version(args.broker_version) < parse_version('0.10.1'):
        raise ValueError("The topic creation API is not usable on brokers older than "
                         "0.10.1. Use --broker_version to specify the version")
    topic_req = CreateTopicRequest(args.topic, args.num_partitions,
                                   args.replication_factor,
                                   args.replica_assignment or [],
                                   args.config_entries or [])
    client.cluster.controller_broker.create_topics([topic_req], args.timeout)


def delete_topic(client, args):
    if parse_version(args.broker_version) < parse_version('0.10.1'):
        raise ValueError("The topic deletion API is not usable on brokers older than "
                         "0.10.1. Use --broker_version to specify the version")
    client.cluster.controller_broker.delete_topics([args.topic], args.timeout)


def _encode_utf8(string):
    """Converts argument to UTF-8-encoded bytes.

    Used to make dict lookups work in Python 3 because topics and things are
    bytes.
    """
    return string.encode('utf-8', errors='replace')


def _add_consumer_group(parser):
    """Add consumer_group to arg parser."""
    parser.add_argument('consumer_group',
                        metavar='CONSUMER_GROUP',
                        help='Consumer group name.',
                        type=_encode_utf8)


def _add_limit(parser):
    """Add limit option to arg parser."""
    parser.add_argument('-l', '--limit',
                        help='Number of messages to consume '
                             '(default: %(default)s)',
                        type=int, default=10)


def _add_timeout(parser):
    parser.add_argument('-t', '--timeout',
                        help='Time in ms to wait for the operation to complete'
                             '(default: %(default)s)',
                        type=int, default=5000)


def _add_offset(parser):
    """Add offset to arg parser."""
    parser.add_argument('offset',
                        metavar='OFFSET',
                        type=str,
                        help='Offset to fetch. Can be EARLIEST, LATEST, or a '
                             'datetime in the format YYYY-MM-DDTHH:MM:SS.')


def _add_outfile(parser):
    """ Add outfile option to arg parser."""
    parser.add_argument('-o', '--outfile',
                        help='output file (defaults to stdout)',
                        type=argparse.FileType('w'), default=sys.stdout)


def _add_topic(parser):
    """Add topic to arg parser."""
    parser.add_argument('topic',
                        metavar='TOPIC',
                        help='Topic name.',
                        type=_encode_utf8)


def _get_arg_parser():
    output = argparse.ArgumentParser(description='Tools for Kafka.')

    # Common arguments
    output.add_argument('-b', '--broker',
                        required=False,
                        default='localhost:9092',
                        dest='host',
                        help='host:port of any Kafka broker. '
                             '[default: localhost:9092]')
    output.add_argument('-o', '--broker_version',
                        required=False,
                        default='0.9.0',
                        dest='broker_version',
                        help="The version string of the broker with which to communicate")

    subparsers = output.add_subparsers(help='Commands', dest='command')

    # Consume to File
    parser = subparsers.add_parser(
        'consume_topic',
        help='Dump messages for a topic to a file or stdout.')
    parser.set_defaults(func=consume_topic)
    _add_topic(parser)
    _add_limit(parser)
    _add_outfile(parser)

    # Desc Topic
    parser = subparsers.add_parser(
        'desc_topic',
        help='Print detailed info for a topic.'
    )
    parser.set_defaults(func=desc_topic)
    _add_topic(parser)

    # Get consumer groups for a topic
    parser = subparsers.add_parser(
        'print_managed_consumer_groups',
        help='Get consumer groups for a topic'
    )
    parser.set_defaults(func=print_managed_consumer_groups)
    _add_topic(parser)

    # Print Consumer Lag
    parser = subparsers.add_parser(
        'print_consumer_lag',
        help='Get consumer lag for a topic.'
    )
    parser.set_defaults(func=print_consumer_lag)
    _add_topic(parser)
    _add_consumer_group(parser)

    # Print Broker Offsets
    parser = subparsers.add_parser(
        'print_offsets',
        help='Fetch broker offsets for a topic'
    )
    parser.set_defaults(func=print_offsets)
    _add_topic(parser)
    _add_offset(parser)

    # Print Topics
    parser = subparsers.add_parser(
        'print_topics',
        help='Print information about all topics in the cluster.'
    )
    parser.set_defaults(func=print_topics)

    # Reset Offsets
    parser = subparsers.add_parser(
        'reset_offsets',
        help='Reset offsets for a topic/consumer group'
    )
    parser.set_defaults(func=reset_offsets)
    _add_topic(parser)
    _add_consumer_group(parser)
    _add_offset(parser)

    parser = subparsers.add_parser(
        'create_topic',
        help='Create a topic'
    )
    parser.set_defaults(func=create_topic)
    _add_topic(parser)
    _add_timeout(parser)
    parser.add_argument('-p', '--num_partitions',
                        help='Number of partitions to be created. -1 indicates unset. '
                             '(default: %(default)s)',
                        type=int, default=1)
    parser.add_argument('-r', '--replication_factor',
                        help='Replication factor for the topic. -1 indicates unset. '
                             '(default: %(default)s)',
                        type=int, default=1)
    parser.add_argument('-a', '--replica_assignment',
                        help='Replica assignment among kafka brokers for this topic '
                             'partitions. If this is set num_partitions and '
                             'replication_factor must be unset. Represent as a JSON '
                             'object with partition IDs as keys as lists of node IDs '
                             'as values',
                        type=_encode_utf8)
    parser.add_argument('-c', '--config_entries',
                        help='Topic level configuration for topic to be set. Represent '
                             'as a JSON object.',
                        type=_encode_utf8)

    parser = subparsers.add_parser(
        'delete_topic',
        help='Delete a topic'
    )
    parser.set_defaults(func=delete_topic)
    _add_topic(parser)
    _add_timeout(parser)

    return output


def main():
    parser = _get_arg_parser()
    args = parser.parse_args()
    if args.command:
        client = pykafka.KafkaClient(hosts=args.host, broker_version=args.broker_version)
        args.func(client, args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
