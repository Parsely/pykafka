"""
Author: Keith Bourgoin, Emmett Butler
"""
__license__ = """
Copyright 2015 Parse.ly, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


class KafkaException(Exception):
    """Generic exception type. The base of all pykafka exception types."""
    pass


class UnicodeException(Exception):
    """Indicates that an error was encountered while processing a unicode string"""
    pass


class NoBrokersAvailableError(KafkaException):
    """Indicates that no brokers were available to the cluster's metadata update attempts
    """
    pass


class LeaderNotFoundError(KafkaException):
    """Indicates that the leader broker for a given partition was not found during
        an update in response to a MetadataRequest
    """
    pass


class SocketDisconnectedError(KafkaException):
    """Indicates that the socket connecting this client to a kafka broker has
        become disconnected
    """
    pass


class ProduceFailureError(KafkaException):
    """Indicates a generic failure in the producer"""
    pass


class ConsumerStoppedException(KafkaException):
    """Indicates that the consumer was stopped when an operation was attempted that required it to be running"""
    pass


class NoMessagesConsumedError(KafkaException):
    """Indicates that no messages were returned from a MessageSet"""
    pass


class MessageSetDecodeFailure(KafkaException):
    """Indicates a generic failure in the decoding of a MessageSet from the broker"""
    pass


class ProducerQueueFullError(KafkaException):
    """Indicates that one or more of the AsyncProducer's internal queues contain at least max_queued_messages messages"""
    pass


class ProducerStoppedException(KafkaException):
    """Raised when the Producer is used while not running"""
    pass


class OffsetRequestFailedError(KafkaException):
    """Indicates that OffsetRequests for offset resetting failed more times than the configured maximum"""
    pass


class PartitionOwnedError(KafkaException):
    """Indicates a given partition is still owned in Zookeeper."""

    def __init__(self, partition, *args, **kwargs):
        super(PartitionOwnedError, self).__init__(*args, **kwargs)
        self.partition = partition


"""
Protocol Client Exceptions
https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes

NOTE: Don't raise these from client code unless it's in direct response to an error
code from the broker. When that's not the case, the exception raised should instead be
a subclass of KafkaException.
"""


class ProtocolClientError(KafkaException):
    """Base class for protocol errors"""
    ERROR_CODE = None


class UnknownError(ProtocolClientError):
    """An unexpected server error"""
    ERROR_CODE = -1


class OffsetOutOfRangeError(ProtocolClientError):
    """The requested offset is outside the range of offsets maintained by the
        server for the given topic/partition.
    """
    ERROR_CODE = 1


class InvalidMessageError(ProtocolClientError):
    """This indicates that a message contents does not match its CRC"""
    ERROR_CODE = 2


class UnknownTopicOrPartition(ProtocolClientError):
    """This request is for a topic or partition that does not exist on this
        broker.
    """
    ERROR_CODE = 3


class InvalidMessageSize(ProtocolClientError):
    """The message has a negative size"""
    ERROR_CODE = 4


class LeaderNotAvailable(ProtocolClientError):
    """This error is thrown if we are in the middle of a leadership election
        and there is currently no leader for this partition and hence it is
        unavailable for writes.
    """
    ERROR_CODE = 5


class NotLeaderForPartition(ProtocolClientError):
    """This error is thrown if the client attempts to send messages to a
        replica that is not the leader for some partition. It indicates that
        the client's metadata is out of date.
    """
    ERROR_CODE = 6


class RequestTimedOut(ProtocolClientError):
    """This error is thrown if the request exceeds the user-specified time
        limit in the request.
    """
    ERROR_CODE = 7


class MessageSizeTooLarge(ProtocolClientError):
    """The server has a configurable maximum message size to avoid unbounded
        memory allocation. This error is thrown if the client attempts to
        produce a message larger than this maximum.
    """
    ERROR_CODE = 10


class OffsetMetadataTooLarge(ProtocolClientError):
    """If you specify a string larger than configured maximum for offset
        metadata
    """
    ERROR_CODE = 12


class GroupLoadInProgress(ProtocolClientError):
    """The broker returns this error code for an offset fetch request if it is
        still loading offsets (after a leader change for that offsets topic
        partition), or in response to group membership requests (such as
        heartbeats) when group metadata is being loaded by the coordinator.
    """
    ERROR_CODE = 14


class GroupCoordinatorNotAvailable(ProtocolClientError):
    """The broker returns this error code for consumer metadata requests or
        offset commit requests if the offsets topic has not yet been created.
    """
    ERROR_CODE = 15


class NotCoordinatorForGroup(ProtocolClientError):
    """The broker returns this error code if it receives an offset fetch or
        commit request for a consumer group that it is not a coordinator for.
    """
    ERROR_CODE = 16


class InvalidTopic(ProtocolClientError):
    """For a request which attempts to access an invalid topic (e.g. one which has
        an illegal name), or if an attempt is made to write to an internal topic
        (such as the consumer offsets topic).
    """
    ERROR_CODE = 17


class IllegalGeneration(ProtocolClientError):
    """Returned from group membership requests (such as heartbeats) when the generation
        id provided in the request is not the current generation
    """
    ERROR_CODE = 22


class InconsistentGroupProtocol(ProtocolClientError):
    """Returned in join group when the member provides a protocol type or set of protocols
        which is not compatible with the current group.
    """
    ERROR_CODE = 23


class UnknownMemberId(ProtocolClientError):
    """Returned from group requests (offset commits/fetches, heartbeats, etc) when the
        memberId is not in the current generation. Also returned if SimpleConsumer is
        incorrectly instantiated with a non-default consumer_id.
    """
    ERROR_CODE = 25


class InvalidSessionTimeout(ProtocolClientError):
    """Returned in join group when the requested session timeout is outside of the allowed
        range on the broker
    """
    ERROR_CODE = 26


class RebalanceInProgress(ProtocolClientError):
    """Returned in heartbeat requests when the coordinator has begun rebalancing the
        group. This indicates to the client that it should rejoin the group.
    """
    ERROR_CODE = 27


class TopicAuthorizationFailed(ProtocolClientError):
    """Returned by the broker when the client is not authorized to access the requested
        topic.
    """
    ERROR_CODE = 29


class GroupAuthorizationFailed(ProtocolClientError):
    """Returned by the broker when the client is not authorized to access a particular
    groupId.
    """
    ERROR_CODE = 30


ERROR_CODES = dict(
    (exc.ERROR_CODE, exc)
    for exc in (UnknownError,
                OffsetOutOfRangeError,
                InvalidMessageError,
                UnknownTopicOrPartition,
                InvalidMessageSize,
                LeaderNotAvailable,
                NotLeaderForPartition,
                RequestTimedOut,
                MessageSizeTooLarge,
                OffsetMetadataTooLarge,
                GroupLoadInProgress,
                GroupCoordinatorNotAvailable,
                NotCoordinatorForGroup,
                InvalidTopic,
                IllegalGeneration,
                InconsistentGroupProtocol,
                UnknownMemberId,
                InvalidSessionTimeout,
                RebalanceInProgress,
                TopicAuthorizationFailed,
                GroupAuthorizationFailed)
)


class RdKafkaException(KafkaException):
    """Error in rdkafka extension that hasn't any equivalent pykafka exception

    In `pykafka.rdkafka._rd_kafka` we try hard to emit the same exceptions
    that the pure pykafka classes emit.  This is a fallback for the few cases
    where we can't find a suitable exception
    """
    pass


class RdKafkaStoppedException(RdKafkaException):
    """Consumer or producer handle was stopped

    Raised by the C extension, to be translated to ConsumerStoppedException or
    ProducerStoppedException by the caller
    """
    pass
