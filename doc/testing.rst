##################
Testing with Samsa
##################

*************
Prerequisites
*************

* `Install Apache ZooKeeper <http://zookeeper.apache.org/releases.html>`_.
* `Install Apache Kafka <http://incubator.apache.org/kafka/downloads.html>`_.

When running tests, both the ``ZOOKEEPER_PATH`` and ``KAFKA_PATH`` environment
variables must be set with the path to the installation directory for the
respective package.

Installation from Apache Development Repositories
=================================================

::

    cd /path/to/project
    git clone git://git.apache.org/zookeeper.git
    git clone git://git.apache.org/kafka.git
    cd kafka
    ./sbt update package
    cd ..
    export ZOOKEEPER_PATH=$PWD/zookeeper
    export KAFKA_PATH=$PWD/kafka

********************
Testing Samsa Itself
********************

To run all tests, run ``make test`` or ``python setup.py test``. To use the
Nose test runner CLI, you can also use ``python setup.py nosetests``.

To run only unit or integration tests, run either ``make unit`` or
``make integration``, respectively. You can also use the presence of the
``integration`` attribute tag on test cases as a filter when testing with Nose.
