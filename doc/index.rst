.. include:: introduction.rst

**************
Help Documents
**************

.. toctree::
    :maxdepth: 1

    usage.rst

*****************
API Documentation
*****************

*Note: PyKafka uses the convention that all class attributes prefixed with an
underscore are considered private. They are not a part of the public interface,
and thus are subject to change without a major version increment at any time.
Class attributes not prefixed with an underscore are treated as a fixed public
API and are only changed in major version increments.*

.. toctree::
    :maxdepth: 2
    :glob:

    api/*
    utils/*

******************
Indices and tables
******************

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
