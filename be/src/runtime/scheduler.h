// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_SCHEDULER_H
#define IMPALA_RUNTIME_SCHEDULER_H

#include <vector>
#include <string>

#include "common/status.h"

namespace impala {

// Abstract scheduler and nameservice class.
// Given a list of resources and locations returns a list of hosts on which
// to execute plan fragments requiring those resources.
// At the moment, this simply returns a list of registered backends running
// on those hosts.
class Scheduler {
 public:
  // Given a list of host names that represent data locations,
  // fills in hostports with host/port pairs of known ImpalaBackendServices
  // (that are running on those hosts or nearby).
  virtual Status GetHosts(
      const std::vector<std::string>& data_locations,
      std::vector<std::pair<std::string, int> >* hostports) = 0;
};

}

#endif
