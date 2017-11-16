import pytest


def patch_subclass(parent, skip_condition):
    """Work around a pytest.mark.skipif bug

    https://github.com/pytest-dev/pytest/issues/568

    The issue causes all subclasses of a TestCase subclass to be skipped if any one
    of them is skipped.

    This fix circumvents the issue by overriding Python's existing subclassing mechanism.
    Instead of having `cls` be a subclass of `parent`, this decorator adds each attribute
    of `parent` to `cls` without using Python inheritance. When appropriate, it also adds
    a boolean condition under which to skip tests for the decorated class.

    :param parent: The "superclass" from which the decorated class should inherit
        its non-overridden attributes
    :type parent: unittest2.TestCase
    :param skip_condition: A boolean condition that, when True, will cause all tests in
        the decorated class to be skipped
    :type skip_condition: bool
    """
    def patcher(cls):
        def build_skipped_method(method, cls, cond=None):
            if cond is None:
                cond = False
            if hasattr(method, "skip_condition"):
                cond = cond or method.skip_condition(cls)

            @pytest.mark.skipif(cond, reason="")
            def _wrapper(self):
                return method(self)
            return _wrapper

        # two passes over parent required so that skips have access to all class
        # attributes
        for attr in parent.__dict__:
            if attr in cls.__dict__:
                continue
            if not attr.startswith("test_"):
                setattr(cls, attr, parent.__dict__[attr])

        for attr in cls.__dict__:
            if attr.startswith("test_"):
                setattr(cls, attr, build_skipped_method(cls.__dict__[attr],
                                                        cls, skip_condition))

        for attr in parent.__dict__:
            if attr.startswith("test_"):
                setattr(cls, attr, build_skipped_method(parent.__dict__[attr],
                                                        cls, skip_condition))
        return cls
    return patcher
