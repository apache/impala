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
#include <glog/logging.h>
#endif

#endif

