# - coding: utf-8 -
import struct
from collections import namedtuple

from ..exceptions import ERROR_CODES
from ..utils import Serializable, ApiVersionAware, struct_helpers


class Request(Serializable, ApiVersionAware):
    """Base class for all Requests. Handles writing header information"""
    HEADER_LEN = 21  # constant for all messages
    CLIENT_ID = b'pykafka'
    API_KEY = -1

    @classmethod
    def get_versions(cls):
        return {}

    def _write_header(self, buff, api_version=0, correlation_id=0):
        """Write the header for an outgoing message.

        :param buff: The buffer into which to write the header
        :type buff: buffer
        :param api_version: The "kafka api version id", used for feature flagging
        :type api_version: int
        :param correlation_id: This is a user-supplied integer. It will be
            passed back in the response by the server, unmodified. It is useful
            for matching request and response between the client and server.
        :type correlation_id: int
        """
        fmt = '!ihhih%ds' % len(self.CLIENT_ID)
        struct.pack_into(fmt, buff, 0,
                         len(buff) - 4,  # msglen excludes this int
                         self.API_KEY,
                         api_version,
                         correlation_id,
                         len(self.CLIENT_ID),
                         self.CLIENT_ID)

    def get_bytes(self):
        """Serialize the message

        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        raise NotImplementedError()


class Response(ApiVersionAware):
    """Base class for Response objects."""
    API_KEY = -1

    @classmethod
    def get_versions(cls):
        return {}

    def raise_error(self, err_code, response):
        """Raise an error based on the Kafka error code

        :param err_code: The error code from Kafka
        :param response: The unpacked raw data from the response
        """
        clsname = str(self.__class__).split('.')[-1].split("'")[0]
        raise ERROR_CODES[err_code](
            'Response Type: "%s"\tResponse: %s' % (
                clsname, response))


class MemberAssignment(object):
    """
    Protocol specification::

    MemberAssignment => Version PartitionAssignment
        Version => int16
        PartitionAssignment => [Topic [Partition]]
            Topic => string
            Partition => int32
        UserData => bytes
    """
    def __init__(self, partition_assignment, version=1):
        self.version = version
        self.partition_assignment = partition_assignment

    @classmethod
    def from_bytestring(cls, buff):
        if len(buff) == 0:
            return cls(tuple())
        fmt = 'h [S [i ] ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        version = response[0]
        partition_assignment = response[1]
        return cls(partition_assignment, version=version)

    def __len__(self):
        # version + len(partition assignment)
        size = 2 + 4
        for topic_name, partitions in self.partition_assignment:
            # len(topic_name) + topic_name + len(partitions)
            size += 2 + len(topic_name) + 4
            size += 4 * len(partitions)
        return size

    def get_bytes(self):
        output = bytearray(len(self))
        offset = 0
        fmt = '!hi'
        struct.pack_into(fmt, output, offset, self.version,
                         len(self.partition_assignment))
        offset += struct.calcsize(fmt)
        for topic_name, partitions in self.partition_assignment:
            fmt = '!h%dsi' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name), topic_name,
                             len(partitions))
            offset += struct.calcsize(fmt)
            for partition_id in partitions:
                fmt = '!i'
                struct.pack_into(fmt, output, offset, partition_id)
                offset += struct.calcsize(fmt)
        return output


class ConsumerGroupProtocolMetadata(object):
    """
    Protocol specification::

    ProtocolMetadata => Version Subscription UserData
        Version => int16
        Subscription => [Topic]
            Topic => string
        UserData => bytes
    """
    def __init__(self, version=0, topic_names=None, user_data=b"testuserdata"):
        self.version = version
        self.topic_names = topic_names or [b"dummytopic"]
        self.user_data = user_data

    def __len__(self):
        # version + len(topic names)
        size = 2 + 4
        for topic_name in self.topic_names:
            # len(topic_name) + topic_name
            size += 2 + len(topic_name)
        # len(user data) + user data
        size += 4 + len(self.user_data)
        return size

    def get_bytes(self):
        output = bytearray(len(self))
        offset = 0
        fmt = '!hi'
        struct.pack_into(fmt, output, offset, self.version, len(self.topic_names))
        offset += struct.calcsize(fmt)
        for topic_name in self.topic_names:
            fmt = '!h%ds' % len(topic_name)
            struct.pack_into(fmt, output, offset, len(topic_name), topic_name)
            offset += struct.calcsize(fmt)
        fmt = '!i%ds' % len(self.user_data)
        struct.pack_into(fmt, output, offset, len(self.user_data), self.user_data)
        offset += struct.calcsize(fmt)
        return output

    @classmethod
    def from_bytestring(cls, buff):
        if len(buff) == 0:
            return cls()
        fmt = 'h [S] Y'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        version = response[0]
        topic_names = response[1]
        user_data = response[2]
        return cls(version, topic_names, user_data)


ApiVersionsSpec = namedtuple('ApiVersionsSpec', ['key', 'min', 'max'])


# Hardcoded API version specifiers for brokers that don't support ApiVersionsRequest
API_VERSIONS_080 = {
    0: ApiVersionsSpec(0, 0, 0),
    1: ApiVersionsSpec(1, 0, 0),
    2: ApiVersionsSpec(2, 0, 0),
    3: ApiVersionsSpec(3, 0, 0),
    4: ApiVersionsSpec(4, 0, 0),
    5: ApiVersionsSpec(5, 0, 0),
    6: ApiVersionsSpec(6, 0, 0),
    7: ApiVersionsSpec(7, 0, 0),
    8: ApiVersionsSpec(8, 0, 1),
    9: ApiVersionsSpec(9, 0, 1),
    10: ApiVersionsSpec(10, 0, 0),
    11: ApiVersionsSpec(11, 0, 0),
    12: ApiVersionsSpec(12, 0, 0),
    13: ApiVersionsSpec(13, 0, 0),
    14: ApiVersionsSpec(14, 0, 0),
    15: ApiVersionsSpec(15, 0, 0),
    16: ApiVersionsSpec(16, 0, 0)
}
API_VERSIONS_090 = {
    0: ApiVersionsSpec(0, 0, 0),
    1: ApiVersionsSpec(1, 0, 1),
    2: ApiVersionsSpec(2, 0, 0),
    3: ApiVersionsSpec(3, 0, 0),
    4: ApiVersionsSpec(4, 0, 0),
    5: ApiVersionsSpec(5, 0, 0),
    6: ApiVersionsSpec(6, 0, 0),
    7: ApiVersionsSpec(7, 0, 0),
    8: ApiVersionsSpec(8, 0, 1),
    9: ApiVersionsSpec(9, 0, 1),
    10: ApiVersionsSpec(10, 0, 0),
    11: ApiVersionsSpec(11, 0, 0),
    12: ApiVersionsSpec(12, 0, 0),
    13: ApiVersionsSpec(13, 0, 0),
    14: ApiVersionsSpec(14, 0, 0),
    15: ApiVersionsSpec(15, 0, 0),
    16: ApiVersionsSpec(16, 0, 0)
}
