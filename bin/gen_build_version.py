#!/usr/bin/env ambari-python-wrap
# This uses system python to avoid a dependency on impala-python,
# because this runs during the build.
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

# This script generates be/src/common/version.h which contains the build version based
# on the git hash.

from __future__ import absolute_import, division, print_function
import os
from subprocess import call

IMPALA_HOME = os.environ['IMPALA_HOME']
SAVE_VERSION_SCRIPT = os.path.join(IMPALA_HOME, 'bin/save-version.sh')
VERSION_FILE_NAME = os.path.join(IMPALA_HOME, 'bin/version.info')
VERSION_CC_FILE_NAME = os.path.join(IMPALA_HOME, 'be/src/common/version.cc')

# Redirecting stdout and stderr to os.devnull as we don't want unnecessary output.
devnull = open(os.devnull, 'w')
try:
  # Force git to look at a git directory relative to IMPALA_HOME,
  # so as to avoid accidentally getting another repo's git hashes.
  can_obtain_git_hash = \
      call(['git', '--git-dir', os.path.join(IMPALA_HOME, ".git"), 'rev-parse', 'HEAD'],
          stdout=devnull, stderr=devnull) == 0
finally:
  devnull.close()

version_file_exists = os.path.isfile(VERSION_FILE_NAME)

# If we have a version file and cannot obtain a git hash, skip generating a new
# version file.
if version_file_exists and not can_obtain_git_hash:
  print("Cannot obtain git hash, using existing version file.")
else:
  # Remove existing version files only if they exist.
  # TODO: Might be useful to make a common utility function remove_if_clean.
  if version_file_exists:
    print('Removing existing file: %s' % (VERSION_FILE_NAME))
    os.remove(VERSION_FILE_NAME)
  if os.path.isfile(VERSION_CC_FILE_NAME):
    print('Removing existing file: %s' % (VERSION_CC_FILE_NAME))
    os.remove(VERSION_CC_FILE_NAME)

  # SAVE_VERSION_SCRIPT will generate a dummy version.info file if we cannot obtain the
  # git hash.
  # Generate a new version file.
  os.system(SAVE_VERSION_SCRIPT)

# version.info file has the format:
# VERSION: <version>
# GIT_HASH: <git has>
# BUILD_TIME: <build time>
version, git_hash, build_time = [None, None, None]
version_file = open(VERSION_FILE_NAME)
try:
  version, git_hash, build_time = [line.split(':',1)[-1].strip() \
      for line in version_file.readlines() if not line.startswith('#') ]
finally:
  version_file.close()

print('\n'.join([version, git_hash, build_time]))

file_contents = """
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This is a generated file, DO NOT EDIT IT.
// To change this file, see impala/bin/gen_build_version.py

#include "common/version.h"

#define IMPALA_BUILD_VERSION "%(build_version)s"
#define IMPALA_BUILD_HASH "%(build_hash)s"
#define IMPALA_BUILD_TIME "%(build_time)s"

const char* GetDaemonBuildVersion() {
  return IMPALA_BUILD_VERSION;
}

const char* GetDaemonBuildHash() {
  return IMPALA_BUILD_HASH;
}

const char* GetDaemonBuildTime() {
  return IMPALA_BUILD_TIME;
}
""" % {'build_version': version,
       'build_hash': git_hash,
       'build_time': build_time}
file_contents = file_contents.strip()

# Generate a new version file.
version_file = open(VERSION_CC_FILE_NAME, "w")
version_file.write(file_contents)
version_file.close()
