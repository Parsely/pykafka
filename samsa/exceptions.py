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


class SamsaException(Exception):
    pass


class ImproperlyConfigured(SamsaException):
    pass


class NoAvailablePartitions(SamsaException):
    pass


class PartitionOwnedException(SamsaException):
    pass


# Protocol Client Exceptions

class ProtocolClientException(SamsaException):
    ERROR_CODE = None


class UnknownError(ProtocolClientException):
    ERROR_CODE = -1


class OffsetOutOfRange(ProtocolClientException):
    ERROR_CODE = 1


class InvalidMessage(ProtocolClientException):
    ERROR_CODE = 2


class WrongPartition(ProtocolClientException):
    ERROR_CODE = 3


class InvalidFetchSize(ProtocolClientException):
    ERROR_CODE = 4


ERROR_CODES = dict((exc.ERROR_CODE, exc) for exc in (UnknownError,
    OffsetOutOfRange, InvalidMessage, WrongPartition, InvalidMessage))
