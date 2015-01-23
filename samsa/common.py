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
