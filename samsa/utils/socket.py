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
