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
import os
import platform

from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError, DistutilsExecError, DistutilsPlatformError
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
from setuptools.extension import Extension

if sys.version_info < (2, 7):
    raise Exception('pykafka requires Python 2.7 or higher.')

python_implementation = platform.python_implementation()
is_cpython = python_implementation == 'CPython'

# Get version without importing, which avoids dependency issues
def get_version():
    with open('pykafka/__init__.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')

install_requires = [
    'six>=1.5',
    'kazoo==2.5.0',
    'tabulate'
]

extra_gevent_requires = [
    'gevent==1.3'
]

extra_requires = [
    'lz4==0.10.1',
    'lz4tools==1.3.1.2',
    'xxhash==1.0.1'
] + extra_gevent_requires

lint_requires = [
    'pep8',
    'pyflakes'
]


def read_lines(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.readlines()

tests_require = [
    x.strip() for x in read_lines('test-requirements.txt') if not x.startswith('-')
]

dependency_links = []
setup_requires = []


class ve_build_ext(build_ext):
    """This class allows C extension building to fail.

    If the name seems horribly random: we've kept it for historic reasons, see
    http://nedbatchelder.com/blog/201212/skipping_c_extensions.html
    This particular evolution was honestly stolen from the sqlalchemy project.
    """
    class BuildFailed(Exception):
        def __init__(self):
            self.cause = sys.exc_info()[1]  # work around py 2/3 different syntax

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            raise self.BuildFailed()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except (CCompilerError, DistutilsExecError, DistutilsPlatformError):
            raise self.BuildFailed()
        except ValueError:
            # this can happen on Windows 64 bit, see Python issue 7511
            if "'path'" in str(sys.exc_info()[1]):  # works with both py 2/3
                raise self.BuildFailed()
            raise


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

if 'nosetests' in sys.argv[1:]:
    setup_requires.append('nose')

with open('README.rst') as f:
    readme = f.read()

def run_setup(with_rdkafka=True):
    """This just calls setuptools.setup() but makes ext_modules optional"""
    ext_modules = []
    if with_rdkafka:
        ext_modules.append(Extension(
            'pykafka.rdkafka._rd_kafka',
            libraries=['rdkafka'],
            sources=['pykafka/rdkafka/_rd_kafkamodule.c']))

    setup(
        name='pykafka',
        version=get_version(),
        author='Keith Bourgoin and Emmett Butler',
        author_email='pykafka-user@googlegroups.com',
        url='https://github.com/Parsely/pykafka',
        description='Full-Featured Pure-Python Kafka Client',
        long_description=readme,
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
            'all': install_requires + tests_require + extra_requires,
            'docs': ['sphinx'] + tests_require,
            'lint': lint_requires,
            'gevent': extra_gevent_requires
        },
        cmdclass={'test': PyTest, 'build_ext': ve_build_ext},
        ext_modules=ext_modules,
        dependency_links=dependency_links,
        zip_safe=False,
        test_suite='nose.collector',
        include_package_data=True,
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python",
            "Programming Language :: Python :: Implementation :: PyPy",
            "Programming Language :: Python :: Implementation :: CPython",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.4",
            "Programming Language :: Python :: 3.5",
            "Programming Language :: Python :: 3.6",
            "Topic :: Database",
            "Topic :: Database :: Front-Ends",
            "Topic :: Software Development :: Libraries :: Python Modules",
        ]
    )

try:
    if not is_cpython:
        print("librdkafka is not supported under %s" % python_implementation)
        print(15 * "-")
        print("INFO: Failed to build rdkafka extension:")
        print("INFO: will now attempt setup without extension.")
        print(15 * "-")
        run_setup(with_rdkafka=False)
    else:
        run_setup()
except ve_build_ext.BuildFailed as exc:
    print(15 * "-")
    print("INFO: Failed to build rdkafka extension:")
    print(exc.cause)
    print("INFO: will now attempt setup without extension.")
    print(15 * "-")
    run_setup(with_rdkafka=False)
