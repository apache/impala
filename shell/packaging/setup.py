#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


"""Set up the Impala shell python package."""

import datetime
import os
import re
import sys
import time

from impala_shell import impala_build_version
from setuptools import find_packages, setup
from textwrap import dedent

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


def parse_requirements(requirements_file='requirements.txt'):
    """
    Parse requirements from the requirements file, stripping comments.

    Args:
      requirements_file: path to a requirements file

    Returns:
      a list of python packages
    """
    lines = []
    with open(requirements_file) as reqs:
        for _ in reqs:
            line = _.split('#')[0]
            if line.strip():
                lines.append(line)
    return lines


def get_version():
  """Generate package version string when calling 'setup.py'.

  When setup.py is being used to CREATE a distribution, e.g., via setup.py sdist
  or setup.py bdist, then use the output from impala_build_version.get_version(),
  and append modifiers as specified by the RELEASE_TYPE and OFFICIAL environment
  variables. By default, the package created will be a dev release, designated
  by timestamp. For example, if get_version() returns the string 3.0.0-SNAPSHOT,
  the package version may be something like 3.0.0.dev20180322154653.

  It's also possible set an evironment variable for BUILD_VERSION to override the
  default build value returned from impala_build_version.get_version().

  E.g., to specify an offical 3.4 beta 2 release (3.4b2), one would call:

    BUILD_VERSION=3.4 RELEASE_TYPE=b2 OFFICIAL=true python setup.py sdist

  The generated version string will be written to a version.txt file to be
  referenced when the distribution is installed.

  When setup.py is invoked during installation, e.g., via pip install or
  setup.py install, read the package version from the version.txt file, which
  is presumed to contain a single line containing a valid PEP-440 version string.
  The file should have been generated when the distribution being installed was
  created. (Although a version.txt file can also be created manually.)

  See https://www.python.org/dev/peps/pep-0440/ for more info on python
  version strings.

  Returns:
    A package version string compliant with PEP-440
  """
  version_file = os.path.join(CURRENT_DIR, 'version.txt')

  if not os.path.isfile(version_file):
    # If setup.py is being executed to create a distribution, e.g., via setup.py
    # sdist or setup.py bdist, then derive the version and WRITE the version.txt
    # file that will later be used for installations.
    if os.getenv('BUILD_VERSION') is not None:
      package_version = os.getenv('BUILD_VERSION')
    else:
      version_match = re.search('\d+\.\d+\.\d+', impala_build_version.get_version())
      if version_match is None:
        sys.exit('Unable to acquire Impala version.')
      package_version = version_match.group(0)

    # packages can be marked as alpha, beta, or rc RELEASE_TYPE
    release_type = os.getenv('RELEASE_TYPE')
    if release_type:
      if not re.match('(a|b|rc)\d+?', release_type):
        msg = """\
            RELEASE_TYPE \'{0}\' does not conform to any PEP-440 release format:

              aN (for alpha releases)
              bN (for beta releases)
              rcN (for release candidates)

            where N is the number of the release"""
        sys.exit(dedent(msg).format(release_type))
      package_version += release_type

    # packages that are not marked OFFICIAL have ".dev" + a timestamp appended
    if os.getenv('OFFICIAL') != 'true':
      epoch_t = time.time()
      ts_fmt = '%Y%m%d%H%M%S'
      timestamp = datetime.datetime.fromtimestamp(epoch_t).strftime(ts_fmt)
      package_version = '{0}.dev{1}'.format(package_version, timestamp)

    with open('version.txt', 'w') as version_file:
      version_file.write(package_version)
  else:
    # If setup.py is being invoked during installation, e.g., via pip install
    # or setup.py install, we expect a version.txt file from which to READ the
    # version string.
    with open(version_file) as version_file:
      package_version = version_file.readline()

  return package_version


setup(
  name='impala_shell',
  python_requires='>2.6',
  version=get_version(),
  description='Impala Shell',
  long_description_content_type='text/markdown',
  long_description=open('README.md').read(),
  author="Impala Dev",
  author_email='dev@impala.apache.org',
  url='https://impala.apache.org/',
  license='Apache Software License',
  packages=find_packages(),
  include_package_data=True,
  install_requires=parse_requirements(),
  entry_points={
    'console_scripts': [
      'impala-shell = impala_shell.impala_shell:impala_shell_main'
    ]
  },
  classifiers=[
    'Development Status :: 5 - Production/Stable',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: End Users/Desktop',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: MacOS :: MacOS X',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python :: 2 :: Only',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Topic :: Database :: Front-Ends'
  ]
)
