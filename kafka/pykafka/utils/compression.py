__license__ = """
Copyright 2014 Parse.ly, Inc.

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

import gzip
import logging

from cStringIO import StringIO

try:
    import snappy
except ImportError:
    snappy = None

logger = logging.getLogger(__name__)


def encode_gzip(buff):
    """Encode a buffer using gzip"""
    sio = StringIO()
    f = gzip.GzipFile(fileobj=sio, mode="w")
    f.write(buff)
    f.close()
    sio.seek(0)
    output = sio.read()
    sio.close()
    return output

def decode_gzip(buff):
    """Decode a gzip buffer"""
    sio = StringIO(buff)
    f = gzip.GzipFile(fileobj=sio, mode='r')
    output = f.read()
    f.close()
    sio.close()
    return output

def encode_snappy(buff):
    """Encode a buffer using Snappy"""
    if snappy is None:
        raise ImportError("Please instally python-snappy")
    output = snappy.compress(buff)
    return snappy.compress(buff)

def decode_snappy(buff):
    """Decode a buffer using Snappy"""
    if snappy is None:
        raise ImportError("Please install python-snappy")
    return snappy.decompress(buff)
