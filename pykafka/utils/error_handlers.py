"""
Author: Emmett Butler
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
__all__ = ["handle_partition_responses", "raise_error"]
from collections import defaultdict
from .compat import iteritems


def handle_partition_responses(error_handlers,
                               parts_by_error=None,
                               success_handler=None,
                               response=None,
                               partitions_by_id=None):
    """Call the appropriate handler for each errored partition

    :param error_handlers: mapping of error code to handler
    :type error_handlers: dict {int: callable(parts)}
    :param parts_by_error: a dict of partitions grouped by error code
    :type parts_by_error: dict
        {int: iterable(:class:`pykafka.simpleconsumer.OwnedPartition`)}
    :param success_handler: function to call for successful partitions
    :type success_handler: callable accepting an iterable of partition responses
    :param response: a Response object containing partition responses
    :type response: :class:`pykafka.protocol.Response`
    :param partitions_by_id: a dict mapping partition ids to OwnedPartition
        instances
    :type partitions_by_id: dict
        {int: :class:`pykafka.simpleconsumer.OwnedPartition`}
    """
    error_handlers = error_handlers.copy()
    if success_handler is not None:
        error_handlers[0] = success_handler

    if parts_by_error is None:
        parts_by_error = build_parts_by_error(response, partitions_by_id)

    for errcode, parts in iteritems(parts_by_error):
        if errcode in error_handlers:
            error_handlers[errcode](parts)

    return parts_by_error


def build_parts_by_error(response, partitions_by_id):
    """Separate the partitions from a response by their error code

    :param response: a Response object containing partition responses
    :type response: :class:`pykafka.protocol.Response`
    :param partitions_by_id: a dict mapping partition ids to OwnedPartition
        instances
    :type partitions_by_id: dict
        {int: :class:`pykafka.simpleconsumer.OwnedPartition`}
    """
    # group partition responses by error code
    parts_by_error = defaultdict(list)
    for topic_name in response.topics.keys():
        for partition_id, pres in iteritems(response.topics[topic_name]):
            owned_partition = None
            if partitions_by_id is not None:
                owned_partition = partitions_by_id[partition_id]
            parts_by_error[pres.err].append((owned_partition, pres))
    return parts_by_error


def raise_error(error, info=""):
    """Raise the given error"""
    raise error(info)
