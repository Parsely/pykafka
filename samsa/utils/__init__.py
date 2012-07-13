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
        pairs = ('%s=%s' % (attribute, repr(getattr(self, attribute, None))) for attribute in attributes)
        return u'<%s.%s at 0x%x: %s>' % (cls.__module__, cls.__name__, id(self), ', '.join(pairs))
    return _repr


