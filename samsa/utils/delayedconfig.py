import functools


def requires_configuration(method):
    """
    A method decorator for objects that derive from ``DelayedConfiguration``
    that ensures methods that require configuration have the appropriate state
    before being invoked.
    """
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        if not self._configured:
            self._configure()
            self._configured = True
        return method(self, *args, **kwargs)
    return wrapped


class DelayedConfiguration(object):
    """
    A mixin class for objects that can be instantiated without their full
    configuration available.
    """
    _configured = False

    def _configure(self):
        raise NotImplementedError
