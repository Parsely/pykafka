"""
Author: Keith Bourgoin
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
__all__ = ["encode_gzip", "decode_gzip", "encode_snappy", "decode_snappy"]
import gzip
from io import BytesIO
import logging
import struct

from .compat import range, buffer, IS_PYPY, PY3

try:
    import snappy
except ImportError:
    snappy = None

log = logging.getLogger(__name__)
# constants used in snappy xerial encoding/decoding
_XERIAL_V1_HEADER = (-126, b'S', b'N', b'A', b'P', b'P', b'Y', 0, 1, 1)
_XERIAL_V1_FORMAT = 'bccccccBii'


def encode_gzip(buff):
    """Encode a buffer using gzip"""
    sio = BytesIO()
    f = gzip.GzipFile(fileobj=sio, mode="w")
    f.write(buff)
    f.close()
    sio.seek(0)
    output = sio.read()
    sio.close()
    return output


def decode_gzip(buff):
    """Decode a buffer using gzip"""
    sio = BytesIO(buff)
    f = gzip.GzipFile(fileobj=sio, mode='r')
    output = f.read()
    f.close()
    sio.close()
    return output


def encode_snappy(buff, xerial_compatible=False, xerial_blocksize=32 * 1024):
    """Encode a buffer using snappy

    If xerial_compatible is set, the buffer is encoded in a fashion compatible
    with the xerial snappy library.

    The block size (xerial_blocksize) controls how frequently the blocking
    occurs. 32k is the default in the xerial library.

    The format is as follows:
    +-------------+------------+--------------+------------+--------------+
    |   Header    | Block1 len | Block1 data  | Blockn len | Blockn data  |
    |-------------+------------+--------------+------------+--------------|
    |  16 bytes   |  BE int32  | snappy bytes |  BE int32  | snappy bytes |
    +-------------+------------+--------------+------------+--------------+

    It is important to note that `blocksize` is the amount of uncompressed
    data presented to snappy at each block, whereas `blocklen` is the
    number of bytes that will be present in the stream.

    Adapted from kafka-python
    https://github.com/mumrah/kafka-python/pull/127/files
    """
    #snappy segfaults if it gets a read-only buffer on PyPy
    if IS_PYPY or PY3:
        buff = bytes(buff)
    if snappy is None:
        raise ImportError("Please install python-snappy")
    if xerial_compatible:
        def _chunker():
            for i in range(0, len(buff), xerial_blocksize):
                yield buff[i:i + xerial_blocksize]
        out = BytesIO()
        full_data = list(zip(_XERIAL_V1_FORMAT, _XERIAL_V1_HEADER))
        header = b''.join(
            [struct.pack('!' + fmt, dat) for fmt, dat in full_data
         ])

        out.write(header)
        for chunk in _chunker():
            block = snappy.compress(chunk)
            block_size = len(block)
            out.write(struct.pack('!i', block_size))
            out.write(block)
        out.seek(0)
        return out.read()
    else:
        return snappy.compress(buff)


def decode_snappy(buff):
    """Decode a buffer using Snappy

    If xerial is found to be in use, the buffer is decoded in a fashion
    compatible with the xerial snappy library.

    Adapted from kafka-python
    https://github.com/mumrah/kafka-python/pull/127/files
    """
    if snappy is None:
        raise ImportError("Please install python-snappy")
    if _detect_xerial_stream(buff):
        out = BytesIO()
        body = buffer(buff[16:])
        if PY3:  # workaround for snappy bug
            body = bytes(body)
        length = len(body)
        cursor = 0
        while cursor < length:
            block_size = struct.unpack_from('!i', body[cursor:])[0]
            cursor += 4
            end = cursor + block_size
            out.write(snappy.decompress(body[cursor:end]))
            cursor = end
        out.seek(0)
        return out.read()
    else:
        return snappy.decompress(buff)


def _detect_xerial_stream(buff):
    """Detects the use of the xerial snappy library

    Returns True if the data given might have been encoded with the blocking
    mode of the xerial snappy library.

    This mode writes a magic header of the format:
        +--------+--------------+------------+---------+--------+
        | Marker | Magic String | Null / Pad | Version | Compat |
        |--------+--------------+------------+---------+--------|
        |  byte  |   c-string   |    byte    |  int32  | int32  |
        |--------+--------------+------------+---------+--------|
        |  -126  |   'SNAPPY'   |     \0     |         |        |
        +--------+--------------+------------+---------+--------+

    `pad` appears to be to ensure that SNAPPY is a valid c-string.
    `version` is the version of this format as written by xerial.
    In the wild, this is currently 1, and as such we only support v1.

    `compat` is there to claim the miniumum supported version that
    can read a xerial block stream; presently in the wild this is 1.

    Adapted from kafka-python
    https://github.com/mumrah/kafka-python/pull/127/files
    """
    if len(buff) > 16:
        header = struct.unpack('!' + _XERIAL_V1_FORMAT, bytes(buff)[:16])
        return header == _XERIAL_V1_HEADER
    return False
