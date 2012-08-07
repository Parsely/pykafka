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

import functools
import logging


logger = logging.getLogger(__name__)


def requires_configuration(method):
    """
    A method decorator for objects that derive from
    :class:`DelayedConfiguration` that ensures methods that require
    configuration have the appropriate state before being invoked.
    """
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        if not self._configured:
            logger.debug('%s requires configuration before %s may be invoked',
                self, method)
            self._configured = True
            self._configure()
        return method(self, *args, **kwargs)
    return wrapped


class DelayedConfiguration(object):
    """
    A mixin class for objects that can be instantiated without their full
    configuration available.

    Subclasses must implement :meth:`_configure`, which will be called once
    to bootstrap the configuration for the each instance.
    """
    _configured = False

    def _configure(self):
        raise NotImplementedError
