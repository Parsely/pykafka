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
import re
import sys

from setuptools import setup, find_packages

# Get version without importing, which avoids dependency issues
def get_version():
    with open('pykafka/__init__.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')

install_requires = [
    'kazoo',
    'tabulate',
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

setup(
    name='pykafka',
    version=get_version(),
    author='Keith Bourgoin and Emmett Butler',
    author_email='pykafka-user@googlegroups.com',
    url='https://github.com/Parsely/pykafka',
    description='Full-Featured Pure-Python Kafka Client',
    keywords='apache kafka client driver',
    license='Apache License 2.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'kafka-tools = pykafka.cli.kafka_tools:main',
            ]
    },
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
    extras_require={
        'test': tests_require,
        'all': install_requires + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': lint_requires
    },
    dependency_links=dependency_links,
    zip_safe=False,
    test_suite='nose.collector',
    include_package_data=True,
    classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 2.7",
            "Topic :: Database",
            "Topic :: Database :: Front-Ends",
            "Topic :: Software Development :: Libraries :: Python Modules",
        ]
)
