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


#ifndef SCHEDULING_SCHEDULER_H
#define SCHEDULING_SCHEDULER_H

#include <vector>
#include <string>

#include "common/global-types.h"
#include "common/status.h"
#include "scheduling/query-schedule.h"
#include "util/container-util.h"
#include "gen-cpp/Types_types.h"  // for TNetworkAddress
#include "gen-cpp/StatestoreService_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/ResourceBrokerService_types.h"

namespace impala {

/// Abstract scheduler and nameservice class.
/// Given a list of resources and locations returns a list of hosts on which
/// to execute plan fragments requiring those resources.
/// At the moment, this simply returns a list of registered backends running
/// on those hosts.
class Scheduler {
 public:
  virtual ~Scheduler() { }

  /// List of server descriptors.
  typedef std::vector<TBackendDescriptor> BackendList;

  /// Given a list of host / port pairs that represent data locations,
  /// fills in hostports with host/port pairs of known ImpalaInternalServices
  /// (that are running on those hosts or nearby).
  virtual Status GetBackends(const std::vector<TNetworkAddress>& data_locations,
      BackendList* backends) = 0;

  /// Return a host/port pair of known ImpalaInternalServices that is running on or
  /// nearby the given data location
  virtual Status GetBackend(const TNetworkAddress& data_location,
      TBackendDescriptor* backend) = 0;

  /// Return true if there is a backend located on the given data_location
  virtual bool HasLocalBackend(const TNetworkAddress& data_location) = 0;

  /// Return a list of all backends known to the scheduler
  virtual void GetAllKnownBackends(BackendList* backends) = 0;

  /// Populates given query schedule whose execution is to be coordinated by coord.
  /// Assigns fragments to hosts based on scan ranges in the query exec request.
  /// If resource management is enabled, also reserves resources from the central
  /// resource manager (Yarn via Llama) to run the query in. This function blocks until
  /// the reservation request has been granted or denied.
  virtual Status Schedule(Coordinator* coord, QuerySchedule* schedule) = 0;

  /// Releases the reserved resources (if any) from the given schedule.
  virtual Status Release(QuerySchedule* schedule) = 0;

  /// Notifies this scheduler that a resource reservation has been preempted by the
  /// central scheduler (Yarn via Llama). All affected queries are cancelled
  /// via their coordinator.
  virtual void HandlePreemptedReservation(const TUniqueId& reservation_id) = 0;

  /// Notifies this scheduler that a single resource with the given client resource id
  /// has been preempted by the central scheduler (Yarn via Llama). All affected queries
  /// are cancelled via their coordinator.
  virtual void HandlePreemptedResource(const TUniqueId& client_resource_id) = 0;

  /// Notifies this scheduler that a single resource with the given client resource id
  /// has been lost by the central scheduler (Yarn via Llama). All affected queries
  /// are cancelled via their coordinator.
  virtual void HandleLostResource(const TUniqueId& client_resource_id) = 0;

  /// Initialises the scheduler, acquiring all resources needed to make
  /// scheduling decisions once this method returns.
  virtual Status Init() = 0;

};

}

#endif
