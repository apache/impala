// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_COMMON_VERSION_H
#define IMPALA_COMMON_VERSION_H

namespace impala {

// This class contains build version information that is set at compile
// time.
class Version {
 public:
  static const char* BUILD_VERSION;
  static const char* BUILD_HASH; 
  static const char* BUILD_TIME;
};

}

#endif

