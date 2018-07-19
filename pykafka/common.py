# - coding: utf-8 -
"""
Author: Keith Bourgoin
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
__all__ = ["Message", "CompressionType", "OffsetType"]
import datetime as dt
import logging


log = logging.getLogger(__name__)
EPOCH = dt.datetime(1970, 1, 1)


class Message(object):
    """Message class.

    :ivar response_code: Response code from Kafka
    :ivar topic: Originating topic
    :ivar payload: Message payload
    :ivar key: (optional) Message key
    :ivar offset: Message offset
    """
    __slots__ = []


class CompressionType(object):
    """Enum for the various compressions supported.

    :cvar NONE: Indicates no compression in use
    :cvar GZIP: Indicates gzip compression in use
    :cvar SNAPPY: Indicates snappy compression in use
    """
    NONE = 0
    GZIP = 1
    SNAPPY = 2
    LZ4 = 3


class OffsetType(object):
    """Enum for special values for earliest/latest offsets.

    :cvar EARLIEST: Indicates the earliest offset available for a partition
    :cvar LATEST: Indicates the latest offset available for a partition
    """
    EARLIEST = -2
    LATEST = -1
