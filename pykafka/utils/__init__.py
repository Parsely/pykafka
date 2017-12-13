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


VERSIONS_CACHE = {}


class ApiVersionAware(object):
    @classmethod
    def get_version_impl(cls, api_versions):
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
