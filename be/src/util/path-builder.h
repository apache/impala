// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_PATH_BUILDER_H
#define IMPALA_UTIL_PATH_BUILDER_H

#include <string>

namespace impala {

// Utility class to construct full paths relative to the impala_home path.
class PathBuilder {
 public:
  // Sets full_path to <IMPALA_HOME>/path
  static void GetFullPath(const std::string& path, std::string* full_path);

  // Sets full_path to <IMPALA_HOME>/<build><debug OR release>/path
  static void GetFullBuildPath(const std::string& path, std::string* full_path);

 private:
  // Cache of env['IMPALA_HOME']
  static const char* impala_home_;

  // Load impala_home_ if it is not already loaded
  static void LoadImpalaHome();
};

}

#endif

