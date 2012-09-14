// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_COMMON_LOGGING_H
#define IMPALA_COMMON_LOGGING_H

// This is a wrapper around the glog header.  When we are compiling to IR,
// we don't want to pull in the glog headers.  Pulling them in causes linking
// issues when we try to dynamically link the codegen'd functions.
#ifdef IR_COMPILE
#include <iostream>
  #define DCHECK(condition) 
  #define DCHECK_EQ(a, b)
  #define DCHECK_NE(a, b)
  #define DCHECK_GT(a, b)
  #define DCHECK_LT(a, b)
  #define DCHECK_GE(a, b)
  #define DCHECK_LE(a, b)
  // Similar to how glog defines DCHECK for release.
  #define LOG(level) while(false) std::cout
  #define VLOG(level) while(false) std::cout 
#else
// glog MUST be included before gflags.  Instead of including them,
// our files should include this file instead.
#include <glog/logging.h>
#endif

// Define VLOG levels.  We want display per-row info less than per-file which
// is less than per-query.  For now per-connection is the same as per-query.
#define VLOG_CONNECTION VLOG(1)
#define VLOG_QUERY      VLOG(1)
#define VLOG_FILE       VLOG(2)
#define VLOG_ROW        VLOG(3)

#define VLOG_CONNECTION_IS_ON VLOG_IS_ON(1)
#define VLOG_QUERY_IS_ON VLOG_IS_ON(1)
#define VLOG_FILE_IS_ON VLOG_IS_ON(2)
#define VLOG_ROW_IS_ON VLOG_IS_ON(3)

#endif

