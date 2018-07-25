# - coding: utf-8 -
from .admin import (ListGroupsRequest, ListGroupsResponse,
                    DescribeGroupsRequest, DescribeGroupsResponse,
                    CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest,
                    DeleteTopicsResponse, ApiVersionsRequest, ApiVersionsResponse,
                    CreateTopicRequest)
from .base import API_VERSIONS_080, API_VERSIONS_090
from .fetch import (PartitionFetchRequest, FetchRequest, FetchPartitionResponse,
                    FetchResponse, FetchResponseV1, FetchResponseV2)
from .group_membership import (JoinGroupRequest, JoinGroupResponse, SyncGroupRequest,
                               SyncGroupResponse, HeartbeatRequest, HeartbeatResponse,
                               LeaveGroupRequest, LeaveGroupResponse,
                               ConsumerGroupProtocolMetadata, MemberAssignment)
from .message import Message, MessageSet
from .metadata import (MetadataRequest, MetadataResponse, MetadataRequestV1,
                       MetadataResponseV1, MetadataRequestV2, MetadataResponseV2,
                       MetadataRequestV3, MetadataResponseV3, MetadataResponseV4,
                       MetadataRequestV4, MetadataRequestV5, MetadataResponseV5)
from .offset import (ListOffsetRequest, ListOffsetResponse, PartitionOffsetRequest,
                     ListOffsetRequestV1, ListOffsetResponseV1)
from .offset_commit import (GroupCoordinatorRequest, GroupCoordinatorResponse,
                            PartitionOffsetCommitRequest, OffsetCommitRequest,
                            OffsetCommitPartitionResponse, OffsetCommitResponse,
                            PartitionOffsetFetchRequest, OffsetFetchRequest,
                            OffsetFetchPartitionResponse, OffsetFetchResponse,
                            OffsetFetchRequestV1, OffsetFetchResponseV1,
                            OffsetFetchRequestV2, OffsetFetchResponseV2)
from .produce import ProduceRequest, ProduceResponse, ProducePartitionResponse

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
           "DeleteTopicsResponse", "PartitionOffsetRequest", "API_VERSIONS_080",
           "API_VERSIONS_090", "ConsumerGroupProtocolMetadata", "MemberAssignment",
           "FetchResponseV1", "FetchResponseV2", "MetadataResponseV1",
           "MetadataRequestV1", "CreateTopicRequest", "ProducePartitionResponse",
           "ListOffsetRequestV1", "ListOffsetResponseV1", "OffsetFetchRequestV1",
           "OffsetFetchResponseV1", "OffsetFetchRequestV2", "OffsetFetchResponseV2",
           "MetadataRequestV2", "MetadataResponseV2", "MetadataRequestV3",
           "MetadataResponseV3", "MetadataRequestV4", "MetadataResponseV4",
           "MetadataRequestV5", "MetadataResponseV5"]
