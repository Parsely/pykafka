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
from pkg_resources import parse_version


class Serializable(object):
    __slots__ = []

    def __len__(self):
        """Length of the bytes that will be sent to the Kafka server."""
        raise NotImplementedError()

    def pack_into(self, buff, offset):
        """Pack serialized bytes into buff starting at offset ``offset``"""
        raise NotImplementedError()


def serialize_utf8(value, partition_key):
    """A serializer accepting bytes or str arguments and returning utf-8 encoded bytes

    Can be used as `pykafka.producer.Producer(serializer=serialize_utf8)`
    """
    if value is not None and type(value) != bytes:
        # allow UnicodeError to be raised here if the encoding fails
        value = value.encode('utf-8')
    if partition_key is not None and type(partition_key) != bytes:
        partition_key = partition_key.encode('utf-8')
    return value, partition_key


def deserialize_utf8(value, partition_key):
    """A deserializer accepting bytes arguments and returning utf-8 strings

    Can be used as `pykafka.simpleconsumer.SimpleConsumer(deserializer=deserialize_utf8)`,
    or similarly in other consumer classes
    """
    # allow UnicodeError to be raised here if the decoding fails
    if value is not None:
        value = value.decode('utf-8')
    if partition_key is not None:
        partition_key = partition_key.decode('utf-8')
    return value, partition_key


VERSIONS_CACHE = {}


class ApiVersionAware(object):
    """Mixin class that facilitates standardized discovery of supported protocol versions
    """
    @classmethod
    def get_version_impl(cls, api_versions):
        """
        Return the class from `pykafka.protocol` implementing support for the highest
        version of the API supported by `cls` supported by both the calling Broker and
        pykafka itself.

        This method requires that `cls` implements the following attributes:
        * cls.get_versions() - a @classmethod taking no arguments aside from `cls` and
            returning a dictionary mapping integer API version numbers to the classes
            from `pykafka.protocol` implementing support for those versions of the
            request or response. For example:
                @classmethod
                def get_versions(cls):
                    return {0: FetchResponse, 1: FetchResponseV1, 2: FetchResponseV2}
            indicates that `FetchResponse` implements support for v0 of the fetch
            response, `FetchResponseV1` implements support for v1 of the fetch response,
            et cetera.
        * cls.API_KEY - a class attribute indicating the API key of the request or
            response. Note that `Response` instances require this attribute despite
            responses not explicitly containing the API key as defined by the Kafka
            protocol.

        :param api_versions: A sequence of :class:`pykafka.protocol.ApiVersionsSpec`
            objects indicating the API version compatibility of the calling Broker
        :type api_versions: Iterable of :class:`pykafka.protocol.ApiVersionsSpec`
        :type return: An object of the same parent class as `cls` (either
            :class:`pykafka.protocol.Request` or :class:`pykafka.protocol.Response`)
        """
        if not hasattr(cls, "get_versions") or not hasattr(cls, "API_KEY"):
            raise AttributeError("get_version_impl requires that {} implement both "
                                 "get_versions() and API_KEY.".format(cls))
        cached_version = VERSIONS_CACHE.get(cls)
        if cached_version:
            return cached_version
        sorted_versions = sorted(cls.get_versions().keys(), reverse=True)
        broker_max = api_versions[cls.API_KEY].max if api_versions else 0
        for version in sorted_versions:
            if version <= broker_max:
                highest_client_supported = cls.get_versions()[version]
                VERSIONS_CACHE[cls] = highest_client_supported
                return highest_client_supported


def msg_protocol_version(broker_version):
    if parse_version(broker_version) >= parse_version("0.10.0"):
        return 1
    return 0
