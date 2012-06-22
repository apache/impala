// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SPARROW_SIMPLE_SCHEDULER_H
#define IMPALA_SPARROW_SIMPLE_SCHEDULER_H

#include <vector>
#include <string>
#include <list>
#include <boost/unordered_map.hpp>

#include "common/status.h"
#include "sparrow/scheduler.h"

namespace sparrow {

// Temporary stand-in for a scheduler, while we're waiting for the Sparrow
// client library.
// Returns hosts "registered" via FLAGS_backends.
class SimpleScheduler : public Scheduler {
 public:
  // Initialize w/ contents of FLAGS_backends.
  SimpleScheduler();
  
  // Returns a list of backends in hostports from FLAGS_backends. For each data_location,
  // the first backend whose host matches is inserted into hostports. If no match is
  // found for a data location, the first backend in the list is inserted.
  virtual impala::Status GetHosts(
      const std::vector<impala::THostPort>& data_locations,
      std::vector<std::pair<std::string, int> >* hostports);

 private:
  // map from host name to list of ports on which ImpalaServiceBackends
  // are listening
  typedef boost::unordered_map<std::string, std::list<int> > HostMap;
  HostMap host_map_;
};

}

#endif
