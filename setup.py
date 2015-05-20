#!/usr/bin/env python
__license__ = """
Copyright 2015 Parse.ly, Inc.

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
import sys

from setuptools import setup, find_packages
from setuptools.extension import Extension

from version import version

install_requires = [
    'kazoo'
]

lint_requires = [
    'pep8',
    'pyflakes'
]

tests_require = ['mock', 'nose', 'unittest2', 'python-snappy']
dependency_links = []
setup_requires = []
if 'nosetests' in sys.argv[1:]:
    setup_requires.append('nose')

rd_kafkamodule = Extension(
    'pykafka.rd_kafka',
    libraries=['rdkafka'],
    sources=['pykafka/rd_kafkamodule.c'])

setup(
    name='pykafka',
    version=version,
    author='Keith Bourgoin',
    author_email='pykafka-user@googlegroups.com',
    url='https://github.com/Parsely/pykafka',
    description='Featureful Kafka client.',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
    extras_require={
        'test': tests_require,
        'all': install_requires + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': lint_requires
    },
    ext_modules=[rd_kafkamodule],
    dependency_links=dependency_links,
    zip_safe=False,
    test_suite='nose.collector',
    include_package_data=True,
)
