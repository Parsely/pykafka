#!/usr/bin/env python
import sys

from setuptools import setup

try:
    import multiprocessing
except ImportError:
    pass

install_requires = [
    'zc-zookeeper-static',
    'kazoo',
]

dependency_links = [
    'https://github.com/python-zk/kazoo/zipball/master#egg=kazoo',
]

tests_require = ['mock', 'nose', 'unittest2']

setup_requires = []
if 'nosetests' in sys.argv[1:]:
    setup_requires.append('nose')

setup(
    name='samsa',
    packages=('samsa',),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
    dependency_links=dependency_links,
    zip_safe=False,
    test_suite='nose.collector',
)
