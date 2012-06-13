// (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains global flags, ie, flags which don't belong to a particular
// component (and would therefore need to be DEFINE'd in every source file containing
// a main()).

#include <glog/logging.h>
#include <gflags/gflags.h>

DEFINE_string(classpath, "", "java classpath");
DEFINE_string(host, "localhost", "The host on which we're running.");
