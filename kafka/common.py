# - coding: utf-8 -

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
