"""
Helpers to convert pykafka config names into librdkafka config names
"""
from copy import copy
import logging

from kafka.common import CompressionType, OffsetType
from kafka.exceptions import ImproperlyConfiguredError
from rd_kafka.config_handles import default_config, default_topic_config


__all__ = ["convert_config"]
logger= logging.getLogger(__name__)


# We can pull most names from rd_kafka, except those that are unset by default:
RD_CONF_NAMES = default_config().keys() + ["group.id"]
RD_TOPIC_CONF_NAMES = default_topic_config().keys()

NOT_AVAILABLE = [ # no equivalent config option in librdkafka
     "offsets_channel_backoff_ms",
     "offsets_channel_socket_timeout_ms",
     "offsets_commit_max_retries",
     "num_consumer_fetchers",
     ]

TRANSLATE_VALUES = { # callable or dict for value conversions
    "auto_commit_enable": {
        True: "true",
        False: "false",
        },
    "auto_offset_reset": {
        OffsetType.EARLIEST: "smallest",
        OffsetType.LATEST: "largest",
        False: "error", # ie refuse to set offset automatically
        },
    "compression": {
        CompressionType.NONE: "none",
        CompressionType.GZIP: "gzip",
        CompressionType.SNAPPY: "snappy",
    },
    "queued_max_messages": lambda conf, n_messages:
        # This is our rough conversion to a "queued.max.messages.kbytes" value;
        # the intent is the same, but we may end up queueing many more messages
        str(n_messages * conf["fetch_message_max_bytes"] // 1024),
}

TRANSLATE_NAMES = { # any names that don't map trivially
    "ack_timeout_ms": "request.timeout.ms",
    "batch_size": "batch.num.messages",
    "compression": "compression.codec",
    "consumer_group": "group.id",
    "max_retries": "message.send.max.retries",
    "queued_max_messages": "queued.max.messages.kbytes",
    "refresh_leader_backoff_ms": "topic.metadata.refresh.fast.interval.ms",
    "required_acks": "request.required.acks",
    "topic_refresh_interval_ms": "topic.metadata.refresh.interval.ms",
    }


def convert_config(config_callargs, base_config=None):
    """ Convert config_callargs to rd_kafka (config, topic_config) tuple """
    config = copy(base_config) if base_config is not None else {}
    topic_config = {}
    for key, val in config_callargs.iteritems():
        if key in NOT_AVAILABLE or val is None:
            logger.debug("Skipping config item: {}={}".format(key, val))
            continue

        # we'll do keys first:
        try:
            renamed_key = TRANSLATE_NAMES[key]
        except KeyError: # just a trivial mapping then:
            renamed_key = key.replace("_", ".")

        # now vals:
        try: # we expect a callable or a dict in TRANSLATE_VALUES:
            converted_val = TRANSLATE_VALUES[key](config_callargs, val)
        except TypeError:
            converted_val = TRANSLATE_VALUES[key][val]
        except KeyError: # no translation, but rdkafka still wants strings:
            converted_val = str(val)

        # split them into two dicts:
        if renamed_key in RD_TOPIC_CONF_NAMES:
            destination = topic_config
        elif renamed_key in RD_CONF_NAMES:
            destination = config
        else:
            raise ImproperlyConfiguredError("{}={} ?".format(key, val))
        destination[renamed_key] = converted_val

    return config, topic_config
