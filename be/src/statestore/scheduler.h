// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef SPARROW_SCHEDULER_H
#define SPARROW_SCHEDULER_H

#include <vector>
#include <string>

#include "common/status.h"
#include "gen-cpp/Types_types.h"  // for TNetworkAddress

namespace impala {

// Abstract scheduler and nameservice class.
// Given a list of resources and locations returns a list of hosts on which
// to execute plan fragments requiring those resources.
// At the moment, this simply returns a list of registered backends running
// on those hosts.
class Scheduler {
 public:
  virtual ~Scheduler() { }

  // List of server addresses. TNetworkAddress.ipdaddress is set, TNetworkAddress.hostname
  // may not be. See IMP-261 for plans to sort this out.
  typedef std::vector<TNetworkAddress> HostList;

  // Given a list of host / port pairs that represent data locations,
  // fills in hostports with host/port pairs of known ImpalaInternalServices
  // (that are running on those hosts or nearby).
  virtual impala::Status GetHosts(
      const HostList& data_locations, HostList* hostports) = 0;

  // Return a host/port pair of known ImpalaInternalServices that is running on or
  // nearby the given data location
  virtual impala::Status GetHost(const TNetworkAddress& data_location,
      TNetworkAddress* hostport) = 0;

  // Return true if there is a host located on the given data_location
  virtual bool HasLocalHost(const TNetworkAddress& data_location) = 0;

  // Return a list of all hosts known to the scheduler
  virtual void GetAllKnownHosts(HostList* hostports) = 0;

  // Initialises the scheduler, acquiring all resources needed to make
  // scheduling decisions once this method returns.
  virtual impala::Status Init() = 0;

  // Scheduler should clean up all resources in Close()
  virtual void Close() = 0;
};

}

#endif
