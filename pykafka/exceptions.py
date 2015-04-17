__license__ = """
Copyright 2012 DISQUS

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
    pass

class ImproperlyConfiguredError(KafkaException):
    pass

class NoAvailablePartitionsError(KafkaException):
    pass

class PartitionOwnedError(KafkaException):
    pass

class InvalidVersionError(KafkaException):
    pass

class MessageTooLargeError(KafkaException):
    pass

class SocketDisconnectedError(KafkaException):
    pass

class ProduceFailureError(KafkaException):
    pass



##
## Protocol Client Exceptions
## https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
##

class ProtocolClientError(KafkaException):
    ERROR_CODE = None

class UnknownError(ProtocolClientError):
    ERROR_CODE = -1

class OffsetOutOfRangeError(ProtocolClientError):
    ERROR_CODE = 1

class InvalidMessageError(ProtocolClientError):
    ERROR_CODE = 2

class UnknownTopicOrPartition(ProtocolClientError):
    ERROR_CODE = 3

class InvalidMessageSize(ProtocolClientError):
    ERROR_CODE = 4

class LeaderNotAvailable(ProtocolClientError):
    ERROR_CODE = 5

class NotLeaderForPartition(ProtocolClientError):
    ERROR_CODE = 6

class RequestTimedOut(ProtocolClientError):
    ERROR_CODE = 7

class BrokerNotAvailable(ProtocolClientError):
    ERROR_CODE = 8

class ReplicaNotAvailable(ProtocolClientError):
    ERROR_CODE = 9

class MessageSizeTooLarge(ProtocolClientError):
    ERROR_CODE = 10

class StaleControllerEpoch(ProtocolClientError):
    ERROR_CODE = 11

class OffsetMetadataTooLarge(ProtocolClientError):
    ERROR_CODE = 12

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
                BrokerNotAvailable,
                ReplicaNotAvailable,
                MessageSizeTooLarge,
                StaleControllerEpoch,
                OffsetMetadataTooLarge)
    )
