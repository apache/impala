#!/usr/bin/env ambari-python-wrap
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


"""This library package contains Impala jenkins-related utilities.
The tooling here is intended for Impala testing and is not installed
as part of production Impala clusters.
"""

from __future__ import absolute_import

from setuptools import find_packages

try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup


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


setup(
  name='impala_py_lib',
  version='0.0.1',
  author_email='dev@impala.apache.org',
  description='Internal python libraries and utilities for Impala development',
  packages=find_packages(),
  include_package_data=True,
  install_requires=parse_requirements(),
  entry_points={
    'console_scripts': [
      'generate-junitxml = impala_py_lib.jenkins.generate_junitxml:main'
    ]
  }
)
