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
to backport those changes to older versions. We like to avoid backporting changes
when possible, but sometimes it's necessary to continue supporting past versions.
In these cases, the changes should be applied on a branch from a checkout of the old
version. This new HEAD should be tagged with the appropriately incremented
version number, and the tag and branch should be pushed to github. After the release
has been created, the branch should be deleted so that only the tagged release remains.

.. _semantic versioning specification: http://semver.org/
