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
    Iterator-based implementation of :func:``.methodmap``.
    """
    fn = methodcaller(name, *args, **kwargs)
    return imap(fn, values)
