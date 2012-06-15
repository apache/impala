// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef SPARROW_SIMPLE_SCHEDULER_H
#define SPARROW_SIMPLE_SCHEDULER_H

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
  // Initialize with a list of <host:port> pairs.
  SimpleScheduler(const std::vector<impala::THostPort>& backends);
  
  // Returns a list of backends such that the impalad at hostports[i] should be used to
  // read data from data_locations[i].
  // For each data_location, we choose a backend whose host matches the data_location in
  // a round robin fashion and insert it into hostports.
  // If no match is found for a data location, assign the data location in round-robin
  // order to any of the backends.
  virtual impala::Status GetHosts(
      const std::vector<impala::THostPort>& data_locations,
      std::vector<std::pair<std::string, int> >* hostports);

 private:
  // map from host name to list of ports on which ImpalaServiceBackends
  // are listening
  typedef boost::unordered_map<std::string, std::list<int> > HostMap;
  HostMap host_map_;

  // round robin entry in HostMap for non-local host assignment
  HostMap::iterator next_nonlocal_host_entry_;
};

}

#endif
