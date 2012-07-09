#!/usr/bin/env python
import sys

from setuptools import setup

try:
    import multiprocessing
except ImportError:
    pass

tests_require = ['mock', 'nose', 'unittest2']

setup_requires = []
if 'nosetests' in sys.argv[1:]:
    setup_requires.append('nose')

setup(
    name='samsa',
    packages=('samsa',),
    tests_require=tests_require,
    setup_requires=setup_requires,
    zip_safe=False,
    test_suite='nose.collector',
)
