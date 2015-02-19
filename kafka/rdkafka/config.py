"""
Helpers to convert pykafka config names into librdkafka config names
"""
import logging

from kafka.exceptions import ImproperlyConfiguredError
from kafka.pykafka.protocol import OFFSET_EARLIEST, OFFSET_LATEST
from rd_kafka.config_handles import default_config, default_topic_config


__all__ = ["convert_config"]
logger= logging.getLogger(__name__)


# We can pull most names from rd_kafka, except those that are unset by default:
RD_CONF_NAMES = default_config().keys() + ["group.id"]
RD_TOPIC_CONF_NAMES = default_topic_config().keys()

NOT_AVAILABLE = [ # no equivalent in librdkafka
     "offsets_channel_backoff_ms",
     "offsets_channel_socket_timeout_ms",
     "offsets_commit_max_retries",
     ]

TRANSLATE_VALUES = { # callable or dict for value conversions
    "auto_commit_enable": {
        True: "true",
        False: "false",
        },
    "auto_offset_reset": {
        OFFSET_EARLIEST: "smallest",
        OFFSET_LATEST: "largest",
        False: "error", # ie refuse to set offset automatically
        },
    "queued_max_message_chunks": lambda conf, chunks:
        str(chunks * conf["fetch_message_max_bytes"] // 1024),
}

TRANSLATE_NAMES = { # any names that don't map trivially
    "consumer_group": "group.id",
    "queued_max_message_chunks": "queued.max.messages.kbytes",
    "refresh_leader_backoff_ms": "topic.metadata.refresh.fast.interval.ms",
    }


def convert_config(config_callargs, base_config={}):
    """
    Convert config_callargs to rd_kafka (config, topic_config) tuple
    """
    config = base_config
    topic_config = {}
    for key, val in config_callargs.iteritems():
        if key in NOT_AVAILABLE or val is None:
            logger.debug("Skipping config item: {}={}".format(key, val))
            continue

        # we'll do keys first:
        try: renamed_key = TRANSLATE_NAMES[key]
        except KeyError: renamed_key = key.replace("_", ".")

        # now vals:
        try: converted_val = TRANSLATE_VALUES[key](config_callargs, val)
        except TypeError: converted_val = TRANSLATE_VALUES[key][val]
        except KeyError: converted_val = str(val)

        # split them into two dicts:
        if renamed_key in RD_TOPIC_CONF_NAMES: destination = topic_config
        elif renamed_key in RD_CONF_NAMES: destination = config
        else: raise ImproperlyConfiguredError("{}={} ?".format(key, val))
        destination[renamed_key] = converted_val

    return config, topic_config
