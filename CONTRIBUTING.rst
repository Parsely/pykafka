PyKafka Contribution Guidelines
===============================

Quick Start
-----------

Get the `source code`_:

::

    git clone https://github.com/Parsely/pykafka.git

.. _source code: https://github.com/Parsely/pykafka

Set up the project for development and run the tests:

::

    python setup.py develop
    tox

Now any changes made in the ``pykafka/`` folder will immediately be reflected in the
pykafka in your environment.

Check the issues list for the `"help wanted"`_ tag if you're interested in finding a place
to contribute.

.. _"help wanted": https://github.com/Parsely/pykafka/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22

Testing
-------

PyKafka uses Tox and pytest for testing. The preferred way to run the tests is via Tox:

::

    pip install tox
    tox

This tests PyKafka under all of the currently supported Python interpreters.

Master should always pass all tests, but branches are allowed to have failing tests.

It can be helpful to set up a git hook to run the tests before pushing to master. This hook lives in `pykafka/.git/hooks/pre-push` and looks like this:

::

    #! /usr/bin/env sh

    BRANCH=`git symbolic-ref -q HEAD`
    if [ "$BRANCH" = "refs/heads/master" ];
    then
        tox
    else
        echo "Not on master. Unit tests not required."
    fi


Pull Request Guidelines
-----------------------

Everything going into the master branch, except the most trivial fixes, should
first start on a feature branch. Feature branches should be named in the format of
``feature/<description>`` where ``<description>`` is some descriptive name for what's
being added. ``bugfix`` and ``enhancement`` prefixes can be used in place of ``feature``
when appropriate.

The Pull Request can be made via the normal GitHub interface and should include
some meaningful description as well as a link to a related Issue, if that exists. The
branch should also include tests when possible.

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
