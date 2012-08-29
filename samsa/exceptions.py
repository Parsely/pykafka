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


class ImproperlyConfiguredError(SamsaException):
    pass


class NoAvailablePartitionsError(SamsaException):
    pass


class PartitionOwnedError(SamsaException):
    pass


class InvalidVersionError(SamsaException):
    pass


# Protocol Client Exceptions

class SocketDisconnectedError(SamsaException):
    pass


class ProtocolClientError(SamsaException):
    # ERROR_CODE as specified by the protocol.
    ERROR_CODE = None


class UnknownError(ProtocolClientError):
    ERROR_CODE = -1


class OffsetOutOfRangeError(ProtocolClientError):
    ERROR_CODE = 1


class InvalidMessageError(ProtocolClientError):
    ERROR_CODE = 2


class WrongPartitionError(ProtocolClientError):
    ERROR_CODE = 3


class InvalidFetchSizeError(ProtocolClientError):
    ERROR_CODE = 4


ERROR_CODES = dict((exc.ERROR_CODE, exc) for exc in (
    UnknownError, OffsetOutOfRangeError, InvalidMessageError,
    WrongPartitionError, InvalidMessageError))
