// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_SIMPLE_SCHEDULER_H
#define IMPALA_RUNTIME_SIMPLE_SCHEDULER_H

#include <vector>
#include <string>
#include <list>
#include <boost/unordered_map.hpp>

#include "common/status.h"
#include "scheduler/scheduler.h"

namespace impala {

// Temporary stand-in for a scheduler, while we're waiting for the Sparrow
// client library.
// Returns hosts "registered" via FLAGS_backends.
class SimpleScheduler : public Scheduler {
 public:
  // Initialize w/ contents of FLAGS_backends.
  SimpleScheduler();
  
  // Returns backends from FLAGS_backends; if an element of data_locations
  // matches a registered host exactly, it returns that host/port, otherwise
  // a random one.
  virtual Status GetHosts(
      const std::vector<std::string>& data_locations,
      std::vector<std::pair<std::string, int> >* hostports);

 private:
  // map from host name to list of ports on which ImpalaServiceBackends
  // are listening
  typedef boost::unordered_map<std::string, std::list<int> > HostMap;
  HostMap host_map_;
};

}

#endif
