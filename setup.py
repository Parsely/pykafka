"""
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
