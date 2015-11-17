import sys
import platform

__all__ = ['PY3', 'Semaphore']

PY3 = sys.version_info[0] >= 3
IS_PYPY = platform.python_implementation().lower() == 'pypy'


def get_bytes(value):
    if hasattr(value, 'encode'):
        try:
            value = value.encode('utf-8')
        except UnicodeError:
            # if we can't encode the value just pass it along
            pass
    return value


def get_string(value):
    if hasattr(value, 'decode'):
        try:
            value = value.decode('utf-8')
        except UnicodeError:
            # if we can't decode the value just pass it along
            pass
    else:
        value = str(value)
    return value

if PY3:
    from threading import Semaphore
    from queue import Queue, Empty  # noqa
    range = range

    def iteritems(d, **kw):
        return iter(d.items(**kw))

    def itervalues(d, **kw):
        return iter(d.values(**kw))

    def iterkeys(d, **kw):
        return iter(d.keys(**kw))

    buffer = memoryview

else:
    range = xrange
    from threading import Condition, Lock
    # could use monotonic.monotonic() backport as well here...
    from time import time as _time
    from Queue import Queue, Empty  # noqa

    def iteritems(d, **kw):
        return d.iteritems(**kw)

    def itervalues(d, **kw):
        return d.itervalues(**kw)

    def iterkeys(d, **kw):
        return d.iterkeys(**kw)

    buffer = buffer

    # -- begin unmodified backport of threading.Semaphore from Python 3.4 -- #
    class Semaphore(object):
        """This class implements semaphore objects.

        Semaphores manage a counter representing the number of release() calls minus
        the number of acquire() calls, plus an initial value. The acquire() method
        blocks if necessary until it can return without making the counter
        negative. If not given, value defaults to 1.

        Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011,
        2012, 2013, 2014, 2015 Python Software Foundation.  All rights reserved.
        """

        # After Tim Peters' semaphore class, but not quite the same (no maximum)

        def __init__(self, value=1):
            if value < 0:
                raise ValueError("semaphore initial value must be >= 0")
            self._cond = Condition(Lock())
            self._value = value

        def acquire(self, blocking=True, timeout=None):
            """Acquire a semaphore, decrementing the internal counter by one.

            When invoked without arguments: if the internal counter is larger than
            zero on entry, decrement it by one and return immediately. If it is zero
            on entry, block, waiting until some other thread has called release() to
            make it larger than zero. This is done with proper interlocking so that
            if multiple acquire() calls are blocked, release() will wake exactly one
            of them up. The implementation may pick one at random, so the order in
            which blocked threads are awakened should not be relied on. There is no
            return value in this case.

            When invoked with blocking set to true, do the same thing as when called
            without arguments, and return true.

            When invoked with blocking set to false, do not block. If a call without
            an argument would block, return false immediately; otherwise, do the
            same thing as when called without arguments, and return true.

            When invoked with a timeout other than None, it will block for at
            most timeout seconds.  If acquire does not complete successfully in
            that interval, return false.  Return true otherwise.

            """
            if not blocking and timeout is not None:
                raise ValueError("can't specify timeout for non-blocking acquire")
            rc = False
            endtime = None
            with self._cond:
                while self._value == 0:
                    if not blocking:
                        break
                    if timeout is not None:
                        if endtime is None:
                            endtime = _time() + timeout
                        else:
                            timeout = endtime - _time()
                            if timeout <= 0:
                                break
                    self._cond.wait(timeout)
                else:
                    self._value -= 1
                    rc = True
            return rc

        __enter__ = acquire

        def release(self):
            """Release a semaphore, incrementing the internal counter by one.

            When the counter is zero on entry and another thread is waiting for it
            to become larger than zero again, wake up that thread.

            """
            with self._cond:
                self._value += 1
                self._cond.notify()

        def __exit__(self, t, v, tb):
            self.release()
    # -- end backport of Semaphore from Python 3.4 -- #
