import time
import unittest2

from samsa.utils.log import get_logger_for_function


class TestCase(unittest2.TestCase):
    """
    A :class:`~unittest2.TestCase` subclass that extra useful test methods.
    """
    def assertPassesWithMultipleAttempts(self, fn, attempts, timeout=1,
            backoff=None, logger=None):
        """
        Executes a callable multiple times, until either the callable exits
        without raising an ``AssertionError``, or the maximum number of
        attempts is exceeded.

        :param fn: test callable
        :type fn: callable
        :param attempts: maximum number of attempts allowed before failing
        :type attempts: ``int``
        :param timeout: number of seconds to wait between attempts
        :type timeout: ``int``
        :param backoff: custom backoff algorithm function that accepts the
            parameters ``(attempt, timeout)``
        :type backoff: callable

        :raises: ``AssertionError`` if an error is encountered on the final
            attempt
        """
        if backoff is None:
            backoff = lambda attempt, timeout: timeout

        if logger is None:
            logger = get_logger_for_function(fn)

        for attempt in xrange(1, attempts + 1):
            logger.debug('Starting attempt %s for %s...', attempt, fn)
            try:
                fn()
                logger.info('Passed attempt %s for %s', attempt, fn)
                break
            except AssertionError:
                if attempt < attempts:
                    wait = backoff(attempt, timeout)
                    logger.exception('Failed attempt %s for %s, waiting for '
                        '%s seconds', attempt, fn, wait)
                    time.sleep(wait)
                else:
                    raise
