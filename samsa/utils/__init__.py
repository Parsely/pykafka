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


def attribute_repr(*attributes):
    """
    Provides an alternative ``__repr__`` implementation that adds the values of
    the given attributes to the output as a development and debugging aid.

    For example::

        >>> class Foo(object):
        ...     __repr__ = attribute_repr('pk', 'slug')
        <foo.models.Foo at 0x100614c50: pk=1, slug=foo>

    :param \*attributes: a sequence of strings that will be used for attribute
        dereferencing on ``self``.
    """
    def _repr(self):
        cls = self.__class__
        pairs = ('%s=%s' % (attribute, repr(getattr(self, attribute, None)))
            for attribute in attributes)
        return u'<%s.%s at 0x%x: %s>' % (
            cls.__module__, cls.__name__, id(self), ', '.join(pairs))
    return _repr
