import gzip
import logging

from cStringIO import StringIO

try:
    import snappy
except ImportError:
    snappy = None

logger = logging.getLogger(__name__)

NONE = 0
GZIP = 1
SNAPPY = 2

def encode_gzip(buff):
    sio = StringIO()
    f = gzip.GzipFile(fileobj=sio, mode="w")
    f.write(buff)
    f.close()
    sio.seek(0)
    output = sio.read()
    sio.close()
    return output

def decode_gzip(buff):
    sio = StringIO(buff)
    f = gzip.GzipFile(fileobj=sio, mode='r')
    output = f.read()
    f.close()
    sio.close()
    return output

def encode_snappy(buff):
    assert snappy is not None
    output = snappy.compress(buff)
    return snappy.compress(buff)

def decode_snappy(buff):
    assert snappy is not None
    return snappy.decompress(buff)
