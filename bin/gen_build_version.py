#!/usr/bin/env impala-python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script generates be/src/common/version.h which contains the build version based
# on the git hash.

import os
import time;
import filecmp
from commands import getstatusoutput
from time import localtime, strftime
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--noclean", action="store_true", default=False,
                  help="If specified, does not remove existing files and only replaces "
                       "them with freshly generated ones if they have changed.")
options, args = parser.parse_args()

IMPALA_HOME = os.environ['IMPALA_HOME']
SAVE_VERSION_SCRIPT = os.path.join(IMPALA_HOME, 'bin/save-version.sh')
VERSION_FILE_NAME = os.path.join(IMPALA_HOME, 'bin/version.info')
VERSION_HEADER_FILE_NAME = os.path.join(IMPALA_HOME, 'be/src/common/version.h')

# Remove existing version files only if --noclean was not specified.
# TODO: Might be useful to make a common utility function remove_if_clean.
if not options.noclean and os.path.isfile(VERSION_FILE_NAME):
  print 'Removing existing file: %s' % (VERSION_FILE_NAME)
  os.remove(VERSION_FILE_NAME)
if not options.noclean and os.path.isfile(VERSION_HEADER_FILE_NAME):
  print 'Removing existing file: %s' % (VERSION_HEADER_FILE_NAME)
  os.remove(VERSION_HEADER_FILE_NAME)

# Generate a new version file only if there is no existing one.
if not os.path.isfile(VERSION_FILE_NAME):
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

print '\n'.join([version, git_hash, build_time])

# construct the build time (e.g. Thu, 04 Oct 2012 11:53:17 PST)
build_time = "%s %s" % (strftime("%a, %d %b %Y %H:%M:%S", localtime()), time.tzname[0])

file_contents = """
// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This is a generated file, DO NOT EDIT IT.
// To change this file, see impala/bin/gen_build_version.py

#ifndef IMPALA_COMMON_VERSION_H
#define IMPALA_COMMON_VERSION_H

#define IMPALA_BUILD_VERSION "%(build_version)s"
#define IMPALA_BUILD_HASH "%(build_hash)s"
#define IMPALA_BUILD_TIME "%(build_time)s"

#endif
""" % {'build_version': version,
       'build_hash': git_hash,
       'build_time': build_time}
file_contents = file_contents.strip()

# Generate a new version file only if there is no existing one.
if not os.path.isfile(VERSION_HEADER_FILE_NAME):
  version_file = open(VERSION_HEADER_FILE_NAME, "w")
  version_file.write(file_contents)
  version_file.close()
