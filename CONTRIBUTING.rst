PyKafka Contribution Guidelines
===============================

Pull Request Guidelines
-----------------------

TODO

Versioning
----------

PyKafka adheres to the `semantic versioning specification`_. It uses version
numbers of the form `X.Y.Z` where X is the major version, Y is the minor version, and
Z is the patch version. Releases with different major versions indicate
changes to the public API.

Past versions of PyKafka are maintained in git with tags. When patches or
private code changes are made to the latest version, it is sometimes desirable
to backport those changes to older versions. In these cases, the changes should
be applied to a checkout of the old version, the minor version or patch number
should be updated accordingly, and a tag for the updated version should be
created.

.. _semantic versioning specification: http://semver.org/
