import time
import unittest2

from samsa.utils.log import get_logger_for_function


class TestCase(unittest2.TestCase):
    def assertPassesWithMultipleAttempts(self, fn, attempts, timeout=1,
            backoff=None, logger=None):
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
