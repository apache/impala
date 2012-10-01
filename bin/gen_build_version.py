#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# This scrips generates be/src/common/version.cc which contains the build version based
# on the git hash.

# Current impala version.
VERSION = "0.1"

import os
import time;
from commands import getstatusoutput
from time import localtime, strftime

IMPALA_HOME = os.environ['IMPALA_HOME']
VERSION_FILE_NAME = os.path.join(IMPALA_HOME, 'bin/version.info')

# If a version file already exists use that, otherwise generate a version info file.
if not os.path.isfile(VERSION_FILE_NAME):
  print 'No version.info file found. Generating new version.info'
  os.system(os.path.join(IMPALA_HOME, 'bin/save-version.sh'))
else:
  print 'Using existing version.info file'

# version.info file has the format:
# VERSION: <version>
# GIT_HASH: <git has>
# BUILD_TIME: <build time>
version, git_hash, build_time = [None, None, None]
with open(VERSION_FILE_NAME) as version_file:
  version, git_hash, build_time = [line.split(':',1)[-1].strip() \
      for line in version_file.readlines() if not line.startswith('#') ]

print '\n'.join([version, git_hash, build_time])

preamble = '\
// Copyright (c) 2012 Cloudera, Inc. All rights reserved.\n\
// This is a generated file, DO NOT EDIT IT.\n\
// To change this file, see impala/bin/gen_build_version.py\n\
\n\
#include "common/version.h"\n\
\n\
using namespace impala;\n\n'

# construct the build time (e.g. Thu, 04 Oct 2012 11:53:17 PST)
build_time = "%s %s" % (strftime("%a, %d %b %Y %H:%M:%S", localtime()), time.tzname[0])

RESULT_PATH = os.environ['IMPALA_HOME'] + '/be/src/common/version.cc'
version_string = "const char* Version::BUILD_VERSION = \"%s\";" % version;
hash_string = "const char* Version::BUILD_HASH = \"%s\";" % git_hash;
time_string = "const char* Version::BUILD_TIME = \"%s\";" % build_time;

version_file = open(RESULT_PATH, "w")
version_file.write(preamble)
version_file.write(version_string + "\n");
version_file.write(hash_string + "\n");
version_file.write(time_string + "\n");
version_file.close()
