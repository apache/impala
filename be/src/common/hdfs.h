// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_COMMON_HDFS_H
#define IMPALA_COMMON_HDFS_H

// This is a wrapper around the hdfs header.  When we are compiling to IR,
// we don't want to pull in the hdfs headers.  We only need the headers
// for the typedefs which we will replicate here
// TODO: is this the cleanest way?  
#ifdef IR_COMPILE
  typedef void* hdfsFS;
  typedef void* hdfsFile;
#else
#include <hdfs.h>
#endif

#endif

