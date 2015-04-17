from collections import defaultdict


def handle_partition_responses(response,
                               error_handlers,
                               success_handler=None,
                               partitions_by_id=None):
    """Call the appropriate handler for each errored partition

    :param response: a Response object containing partition responses
    :type response: pykafka.protocol.Response
    :param success_handler: function to call for successful partitions
    :type success_handler: callable(parts)
    :param error_handlers: mapping of error code to handler
    :type error_handlers: dict {int: callable(parts)}
    :param partitions_by_id: a dict mapping partition ids to OwnedPartition
        instances
    :type partitions_by_id: dict {int: pykafka.simpleconsumer.OwnedPartition}
    """
    error_handlers = error_handlers.copy()
    if success_handler is not None:
        error_handlers[0] = success_handler

    # group partition responses by error code
    parts_by_error = defaultdict(list)
    for topic_name in response.topics.keys():
        for partition_id, pres in response.topics[topic_name].iteritems():
            owned_partition = None
            if partitions_by_id is not None:
                owned_partition = partitions_by_id[partition_id]
            parts_by_error[pres.error].append((owned_partition, pres))

    for errcode, parts in parts_by_error.iteritems():
        if errcode in error_handlers:
            error_handlers[errcode](parts)

    return parts_by_error
