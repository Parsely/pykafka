from __future__ import division
"""
Author: Keith Bourgoin, Emmett Butler
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
__all__ = ["BrokerConnection"]
import logging
import socket
import struct

from .exceptions import SocketDisconnectedError
from .utils.socket import recvall_into
from .utils.compat import buffer

log = logging.getLogger(__name__)


class BrokerConnection(object):
    """
    BrokerConnection thinly wraps a `socket.create_connection` call
    and handles the sending and receiving of data that conform to the
    kafka binary protocol over that socket.
    """
    def __init__(self,
                 host,
                 port,
                 buffer_size=1024 * 1024,
                 source_host='',
                 source_port=0):
        """Initialize a socket connection to Kafka.

        :param host: The host to which to connect
        :type host: str
        :param port: The port on the host to which to connect
        :type port: int
        :param buffer_size: The size (in bytes) of the buffer in which to
            hold response data.
        :type buffer_size: int
        :param source_host: The host portion of the source address for
            the socket connection
        :type source_host: str
        :param source_port: The port portion of the source address for
            the socket connection
        :type source_port: int
        """
        self._buff = bytearray(buffer_size)
        self.host = host
        self.port = port
        self._socket = None
        self.source_host = source_host
        self.source_port = source_port

    def __del__(self):
        """Close this connection when the object is deleted."""
        self.disconnect()

    @property
    def connected(self):
        """Returns true if the socket connection is open."""
        return self._socket is not None

    def connect(self, timeout):
        """Connect to the broker."""
        log.debug("Connecting to %s:%s", self.host, self.port)
        self._socket = socket.create_connection(
            (self.host, self.port),
            timeout / 1000,
            (self.source_host, self.source_port)
        )
        if self._socket is not None:
            log.debug("Successfully connected to %s:%s", self.host, self.port)

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
        """Disconnect from the broker, then reconnect"""
        self.disconnect()
        self.connect()

    def request(self, request):
        """Send a request over the socket connection"""
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
