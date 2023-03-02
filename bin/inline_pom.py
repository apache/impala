#!/usr/bin/env impala-python
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
#
# This script can be used to inline environment variables in pom.xml
#
# Usage: inline_pom.py <pom.xml>...

from __future__ import absolute_import, division, print_function
import re
import sys
from tempfile import mkstemp
from shutil import move, copymode
from os import fdopen, remove, getenv

env_pattern = re.compile(r"\$\{env\.[^}]*\}")


class colors:
  HEADER = '\033[95m'
  WARNING = '\033[93m'
  ENDC = '\033[0m'


def inline_envs(line):
  envs = env_pattern.findall(line)
  for env in envs:
    name = env[6:-1]
    value = getenv(name)
    print("{}={}".format(name, value))
    if value:
      line = line.replace(env, value)
  return line


def update_pom(pom_path):
  fd, new_path = mkstemp()
  with fdopen(fd, 'w') as new_file:
    with open(pom_path, 'r') as old_file:
      for line in old_file:
        new_file.write(inline_envs(line))

  copymode(pom_path, new_path)
  remove(pom_path)
  move(new_path, pom_path)


if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("Usage: inline_pom.py <pom.xml>...")
    sys.exit(1)

  print("{}Replacing environment variables in {}{}"
      .format(colors.HEADER, ', '.join(sys.argv[1:]), colors.ENDC))
  print("{}WARNING: avoid committing updated POM files.{}"
      .format(colors.WARNING, colors.ENDC))
  for pom_path in sys.argv[1:]:
    update_pom(pom_path)
