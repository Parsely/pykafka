__license__ = """
Copyright 2012 DISQUS

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

from samsa.exceptions import SocketDisconnectedError


def recvall_into(socket, bytea):
    """
    Reads enough data from the socket to fill the provided bytearray (modifies
    in-place.)

    This is basically a hack around the fact that ``socket.recv_into`` doesn't
    allow buffer offsets.

    :type socket: :class:`socket.Socket`
    :type bytea: ``bytearray``
    :rtype: ``bytearray``
    """
    offset = 0
    size = len(bytea)
    while offset < size:
        remaining = size - offset
        chunk = socket.recv(remaining)
        if not len(chunk):
            raise SocketDisconnectedError
        bytea[offset:(offset + len(chunk))] = chunk
        offset += len(chunk)
    return bytea


def recv_struct(socket, struct):
    """
    Reads enough data from the socket to unpack the given struct.

    :type socket: :class:`socket.Socket`
    :type struct: :class:`struct.Struct`
    :rtype: ``tuple``
    """
    bytea = bytearray(struct.size)
    recvall_into(socket, bytea)
    return struct.unpack_from(buffer(bytea))


def recv_framed(socket, framestruct):
    """
    :type socket: :class:`socket.Socket`
    :type frame: :class:`struct.Struct`
    :rtype: ``bytearray``
    """
    (size,) = recv_struct(socket, framestruct)
    return recvall_into(socket, bytearray(size))
