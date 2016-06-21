import platform

import pytest

from tests.pykafka import test_ssl


@pytest.mark.skipif(platform.python_implementation() == "PyPy",
                    reason="We pass PyObject pointers as msg_opaques for "
                           "delivery callbacks, which is unsafe on PyPy.")
class TestRdKafkaSsl(test_ssl.SslIntegrationTests):
    USE_RDKAFKA = True
