from __future__ import division

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

# TODO: Gevent support

import logging
import socket
import struct

from .exceptions import SocketDisconnectedError
from .utils.socket import recvall_into

logger = logging.getLogger(__name__)


class BrokerConnection(object):
    """A socket connection to Kafka."""

    def __init__(self, host, port, buffer_size=64 * 1024):
        self._buff = bytearray(buffer_size)
        self.host = host
        self.port = port
        self._socket = None

    def __del__(self):
        self.disconnect()

    @property
    def connected(self):
        """Do we think the socket is open."""
        return self._socket is not None

    def connect(self, timeout):
        """Connect to the broker."""
        self._socket = socket.create_connection(
            (self.host, self.port),
            timeout=timeout / 1000
        )

    def disconnect(self):
        """Disconnect from the broker."""
        if self._socket is None:
            return
        try:
            self._socket.close()
        except IOError:
            pass
        finally:
            self._socket = None

    def reconnect(self):
        self.disconnect()
        self.connect()

    def request(self, request):
        bytes = request.get_bytes()
        if not self._socket:
            raise SocketDisconnectedError
        self._socket.sendall(bytes)

    def response(self):
        """Wait for a response from the broker"""
        size = self._socket.recv(4)
        if len(size) == 0:
            # Happens when broker has shut down
            self.disconnect()
            raise SocketDisconnectedError
        size = struct.unpack('!i', size)[0]
        recvall_into(self._socket, self._buff, size)
        return buffer(self._buff[4:4 + size])
