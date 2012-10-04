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

status,GIT_HASH = getstatusoutput('git rev-parse HEAD')
if status != 0:
  print "Couldn't get git hash.  Are you in a git repository?"
  exit()

preamble = '\
// Copyright (c) 2011 Cloudera, Inc. All rights reserved.\n\
// This is a generated file, DO NOT EDIT IT.\n\
// To change this file, see impala/bin/gen_build_version.py\n\
\n\
#include "common/version.h"\n\
\n\
using namespace impala;\n\n'

# construct the build time (e.g. Thu, 04 Oct 2012 11:53:17 PST)
build_time = "%s %s" % (strftime("%a, %d %b %Y %H:%M:%S", localtime()), time.tzname[0])

RESULT_PATH = os.environ['IMPALA_HOME'] + '/be/src/common/version.cc'
version_string = "const char* Version::BUILD_VERSION = \"%s\";" % VERSION;
hash_string = "const char* Version::BUILD_HASH = \"%s\";" % GIT_HASH;
time_string = "const char* Version::BUILD_TIME = \"%s\";" % build_time;

version_file = open(RESULT_PATH, "w")
version_file.write(preamble)
version_file.write(version_string + "\n");
version_file.write(hash_string + "\n");
version_file.write(time_string + "\n");
version_file.close()
