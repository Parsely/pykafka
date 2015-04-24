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
import logging


logger = logging.getLogger(__name__)


class Message(object):
    """Message class.

    :ivar response_code: Response code from Kafka
    :ivar topic: Originating topic
    :ivar payload: Message payload
    :ivar key: (optional) Message key
    :ivar offset: Message offset
    """
    pass

class CompressionType(object):
    """Enum for the various compressions supported."""
    NONE = 0
    GZIP = 1
    SNAPPY = 2

class OffsetType(object):
    """Enum for special values for earliest/latest offsets."""
    EARLIEST = -2
    LATEST = -1
