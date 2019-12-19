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
from __future__ import print_function, unicode_literals

"""
A module where we can aggregate python2 -> 3 code contortions.
"""

import os
import sys


if sys.version_info.major == 2:
  # default is typically ASCII, but unicode_literals dictates UTF-8
  # See also https://stackoverflow.com/questions/492483/setting-the-correct-encoding-when-piping-stdout-in-python  # noqa
  os.environ['PYTHONIOENCODING'] = 'utf-8'


try:
  _xrange = xrange
except NameError:
  _xrange = range  # python3 compatibilty


try:
  _basestring = basestring
except NameError:
  _basestring = str  # python3 compatibility
