# - coding: utf-8 -
import struct

from .base import Request, Response
from ..utils import struct_helpers


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


class JoinGroupRequest(Request):
    """A group join request
    Specification::
    JoinGroupRequest => GroupId SessionTimeout MemberId ProtocolType GroupProtocols
        GroupId => string
        SessionTimeout => int32
        MemberId => string
        ProtocolType => string
        GroupProtocols => [ProtocolName ProtocolMetadata]
            ProtocolName => string
            ProtocolMetadata => bytes
    """
    API_KEY = 11

    def __init__(self,
                 group_id,
                 member_id,
                 topic_name,
                 membership_protocol,
                 session_timeout=30000):
        """Create a new group join request"""
        self.protocol = membership_protocol
        self.group_id = group_id
        self.session_timeout = session_timeout
        self.member_id = member_id
        self.protocol_type = self.protocol.protocol_type
        self.group_protocols = [(self.protocol.protocol_name,
                                 bytes(self.protocol.metadata.get_bytes()))]

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + group id + session timeout
        size = self.HEADER_LEN + 2 + len(self.group_id) + 4
        # + member id + protocol type + len(group protocols)
        size += 2 + len(self.member_id) + 2 + len(self.protocol_type) + 4
        # metadata tuples
        for name, metadata in self.group_protocols:
            size += 2 + len(name) + 4 + len(metadata)
        return size

    def get_bytes(self):
        """Serialize the message
        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!h%dsih%dsh%dsi' % (len(self.group_id), len(self.member_id),
                                   len(self.protocol_type))
        struct.pack_into(fmt, output, offset, len(self.group_id), self.group_id,
                         self.session_timeout, len(self.member_id), self.member_id,
                         len(self.protocol_type), self.protocol_type,
                         len(self.group_protocols))
        offset += struct.calcsize(fmt)
        for protocol_name, protocol_metadata in self.group_protocols:
            fmt = '!h%dsi%ds' % (len(protocol_name), len(protocol_metadata))
            struct.pack_into(fmt, output, offset, len(protocol_name), protocol_name,
                             len(protocol_metadata), protocol_metadata)
            offset += struct.calcsize(fmt)
        return output


class JoinGroupResponse(Response):
    """A group join response
    Specification::
    JoinGroupResponse => ErrorCode GenerationId GroupProtocol LeaderId MemberId Members
        ErrorCode => int16
        GenerationId => int32
        GroupProtocol => string
        LeaderId => string
        MemberId => string
        Members => [MemberId MemberMetadata]
            MemberId => string
            MemberMetadata => bytes
    """
    def __init__(self, buff):
        """Deserialize into a new Response
        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'hiSSS[SY ]'
        response = struct_helpers.unpack_from(fmt, buff, 0)

        self.error_code = response[0]
        self.generation_id = response[1]
        self.group_protocol = response[2]
        self.leader_id = response[3]
        self.member_id = response[4]
        self.members = {_id: ConsumerGroupProtocolMetadata.from_bytestring(meta)
                        for _id, meta in response[5]}


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


class SyncGroupRequest(Request):
    """A group sync request
    Specification::
    SyncGroupRequest => GroupId GenerationId MemberId GroupAssignment
        GroupId => string
        GenerationId => int32
        MemberId => string
        GroupAssignment => [MemberId MemberAssignment]
            MemberId => string
            MemberAssignment => bytes
    """
    API_KEY = 14

    def __init__(self, group_id, generation_id, member_id, group_assignment):
        """Create a new group join request"""
        self.group_id = group_id
        self.generation_id = generation_id
        self.member_id = member_id
        self.group_assignment = group_assignment

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + len(group id) + group id + generation id
        size = self.HEADER_LEN + 2 + len(self.group_id) + 4
        # + len(member id) + member id + len(group assignment)
        size += 2 + len(self.member_id) + 4
        # group assignment tuples
        for member_id, member_assignment in self.group_assignment:
            # + len(member id) + member id + len(member assignment) + member assignment
            size += 2 + len(member_id) + 4 + len(member_assignment)
        return size

    def get_bytes(self):
        """Serialize the message
        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!h%dsih%dsi' % (len(self.group_id), len(self.member_id))
        struct.pack_into(fmt, output, offset, len(self.group_id), self.group_id,
                         self.generation_id, len(self.member_id), self.member_id,
                         len(self.group_assignment))
        offset += struct.calcsize(fmt)
        for member_id, member_assignment in self.group_assignment:
            assignment_bytes = bytes(member_assignment.get_bytes())
            fmt = '!h%dsi%ds' % (len(member_id), len(assignment_bytes))
            struct.pack_into(fmt, output, offset, len(member_id), member_id,
                             len(assignment_bytes), assignment_bytes)
            offset += struct.calcsize(fmt)
        return output


class SyncGroupResponse(Response):
    """A group sync response
    Specification::
    SyncGroupResponse => ErrorCode MemberAssignment
        ErrorCode => int16
        MemberAssignment => bytes
    """
    def __init__(self, buff):
        """Deserialize into a new Response
        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'hY'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        self.error_code = response[0]
        self.member_assignment = MemberAssignment.from_bytestring(response[1])


class HeartbeatRequest(Request):
    """A group heartbeat request
    Specification::
    HeartbeatRequest => GroupId GenerationId MemberId
        GroupId => string
        GenerationId => int32
        MemberId => string
    """
    API_KEY = 12

    def __init__(self, group_id, generation_id, member_id):
        """Create a new heartbeat request"""
        self.group_id = group_id
        self.generation_id = generation_id
        self.member_id = member_id

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + len(group id) + group id + generation id
        size = self.HEADER_LEN + 2 + len(self.group_id) + 4
        # + len(member id) + member id
        size += 2 + len(self.member_id)
        return size

    def get_bytes(self):
        """Serialize the message
        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!h%dsih%ds' % (len(self.group_id), len(self.member_id))
        struct.pack_into(fmt, output, offset, len(self.group_id), self.group_id,
                         self.generation_id, len(self.member_id), self.member_id)
        offset += struct.calcsize(fmt)
        return output


class HeartbeatResponse(Response):
    """A group heartbeat response
    Specification::
    HeartbeatResponse => ErrorCode
        ErrorCode => int16
    """
    def __init__(self, buff):
        """Deserialize into a new Response
        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'h'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        self.error_code = response[0]


class LeaveGroupRequest(Request):
    """A group exit request
    Specification::
    LeaveGroupRequest => GroupId MemberId
        GroupId => string
        MemberId => string
    """
    API_KEY = 13

    def __init__(self, group_id, member_id):
        """Create a new group join request"""
        self.group_id = group_id
        self.member_id = member_id

    def __len__(self):
        """Length of the serialized message, in bytes"""
        # Header + len(group id) + group id
        size = self.HEADER_LEN + 2 + len(self.group_id)
        # + len(member id) + member id
        size += 2 + len(self.member_id)
        return size

    def get_bytes(self):
        """Serialize the message
        :returns: Serialized message
        :rtype: :class:`bytearray`
        """
        output = bytearray(len(self))
        self._write_header(output)
        offset = self.HEADER_LEN
        fmt = '!h%dsh%ds' % (len(self.group_id), len(self.member_id))
        struct.pack_into(fmt, output, offset, len(self.group_id), self.group_id,
                         len(self.member_id), self.member_id)
        offset += struct.calcsize(fmt)
        return output


class LeaveGroupResponse(Response):
    """A group exit response
    Specification::
    LeaveGroupResponse => ErrorCode
        ErrorCode => int16
    """
    def __init__(self, buff):
        """Deserialize into a new Response
        :param buff: Serialized message
        :type buff: :class:`bytearray`
        """
        fmt = 'h'
        response = struct_helpers.unpack_from(fmt, buff, 0)
        self.error_code = response[0]
