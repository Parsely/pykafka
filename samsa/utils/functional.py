from operator import methodcaller
from itertools import imap


def methodmap(name, values, *args, **kwargs):
    """
    Maps over all members in ``values``, invoking the method named ``name``.

    Usage:
    >>> methodmap('strip', ['hello ', 'world '])
    ['hello', 'world']
    """
    fn = methodcaller(name, *args, **kwargs)
    return map(fn, values)


def methodimap(name, values, *args, **kwargs):
    """
    Iterator-based implementation of :func:``samsa.utils.functional.methodmap``.
    """
    fn = methodcaller(name, *args, **kwargs)
    return imap(fn, values)
