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
__all__ = ["recvall_into"]
from pykafka.exceptions import SocketDisconnectedError


def recvall_into(socket, bytea, size):
    """
    Reads `size` bytes from the socket into the provided bytearray (modifies
    in-place.)

    This is basically a hack around the fact that `socket.recv_into` doesn't
    allow buffer offsets.

    :type socket: :class:`socket.Socket`
    :type bytea: ``bytearray``
    :type size: int
    :rtype: `bytearray`
    """
    offset = 0
    while offset < size:
        remaining = size - offset
        try:
            chunk = socket.recv(remaining)
        except IOError:
            chunk = None
        if chunk is None or len(chunk) == 0:
            raise SocketDisconnectedError
        bytea[offset:(offset + len(chunk))] = chunk
        offset += len(chunk)
    return bytea
