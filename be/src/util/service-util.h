// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_SERVICE_UTIL_H
#define IMPALA_UTIL_SERVICE_UTIL_H

#include "util/webserver.h"

namespace impala {

// Utility functions for services
class ServiceUtil {
 public:
  // Webserver callback. Prints a dump of the memory usage for this services/process.
  // Currently, this just outputs the information from tcmalloc.
  static void RenderMemUsage(const Webserver::ArgumentMap& args,
      std::stringstream* output);
};

}
#endif
