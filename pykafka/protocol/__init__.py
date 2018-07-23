# - coding: utf-8 -
from admin import (ListGroupsRequest, ListGroupsResponse,
                   DescribeGroupsRequest, DescribeGroupsResponse,
                   CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest,
                   DeleteTopicsResponse, ApiVersionsRequest, ApiVersionsResponse)
from fetch import (PartitionFetchRequest, FetchRequest, FetchPartitionResponse,
                   FetchResponse)
from group_membership import (JoinGroupRequest, JoinGroupResponse, SyncGroupRequest,
                              SyncGroupResponse, HeartbeatRequest, HeartbeatResponse,
                              LeaveGroupRequest, LeaveGroupResponse)
from message import Message, MessageSet
from metadata import MetadataRequest, MetadataResponse
from offset import ListOffsetRequest, ListOffsetResponse
from offset_commit import (GroupCoordinatorRequest, GroupCoordinatorResponse,
                           PartitionOffsetCommitRequest, OffsetCommitRequest,
                           OffsetCommitPartitionResponse, OffsetCommitResponse,
                           PartitionOffsetFetchRequest, OffsetFetchRequest,
                           OffsetFetchPartitionResponse, OffsetFetchResponse)
from produce import ProduceRequest, ProduceResponse

"""
Author: Keith Bourgoin, Emmett Butler

Protocol implementation for Kafka>=0.8.2

The implementation has been done with an attempt to minimize memory
allocations in order to improve performance. With the exception of
compressed messages, we can calculate the size of the entire message
to send and do only a single memory allocation.

For Reference:

https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

Each message is encoded as either a Request or Response:

RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32

RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
  ApiKey => int16
  ApiVersion => int16
  CorrelationId => int32
  ClientId => string
  RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest

Response => CorrelationId ResponseMessage
  CorrelationId => int32
  ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse
"""
__all__ = ["MetadataRequest", "MetadataResponse", "ProduceRequest", "ProduceResponse",
           "PartitionFetchRequest", "FetchRequest", "FetchPartitionResponse",
           "FetchResponse", "ListOffsetRequest", "ListOffsetResponse",
           "GroupCoordinatorRequest", "GroupCoordinatorResponse",
           "PartitionOffsetCommitRequest", "OffsetCommitRequest",
           "OffsetCommitPartitionResponse", "OffsetCommitResponse",
           "PartitionOffsetFetchRequest", "OffsetFetchRequest",
           "OffsetFetchPartitionResponse", "OffsetFetchResponse",
           "JoinGroupRequest", "JoinGroupResponse", "SyncGroupRequest",
           "SyncGroupResponse", "HeartbeatRequest", "HeartbeatResponse",
           "LeaveGroupRequest", "LeaveGroupResponse", "ListGroupsRequest",
           "ListGroupsResponse", "DescribeGroupsRequest", "DescribeGroupsResponse",
           "Message", "MessageSet", "ApiVersionsRequest", "ApiVersionsResponse",
           "CreateTopicsRequest", "CreateTopicsResponse", "DeleteTopicsRequest",
           "DeleteTopicsResponse"]
