// (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains global flags, ie, flags which don't belong to a particular
// component (and would therefore need to be DEFINE'd in every source file containing
// a main()).

#include <glog/logging.h>
#include <gflags/gflags.h>

DEFINE_string(classpath, "", "java classpath");
DEFINE_string(host, "localhost", "The host on which we're running.");
DEFINE_string(planservice_host, "localhost", "Host on which planservice is running");
DEFINE_int32(planservice_port, 20000, "Port on which planservice is running");
