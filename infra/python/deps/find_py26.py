#!/usr/bin/python
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

# This script finds Python 2.6 or higher on the system and outputs the
# system command to stdout. The script exits with a nonzero exit code if
# Python 2.6+ is not present.

from __future__ import print_function
import os
import subprocess
import sys
import textwrap

def detect_python_cmd():
  '''Returns the system command that provides python 2.6 or greater.'''
  paths = os.getenv("PATH").split(os.path.pathsep)
  for cmd in ("python", "python27", "python2.7", "python-27", "python-2.7", "python26",
      "python2.6", "python-26", "python-2.6"):
    for path in paths:
      cmd_path = os.path.join(path, cmd)
      if not os.path.exists(cmd_path) or not os.access(cmd_path, os.X_OK):
        continue
      exit = subprocess.call([cmd_path, "-c", textwrap.dedent("""
          import sys
          sys.exit(int(sys.version_info[:2] < (2, 6)))""")])
      if exit == 0:
        return cmd_path
  raise Exception("Could not find minimum required python version 2.6")


print(detect_python_cmd())
