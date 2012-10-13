// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef SPARROW_SIMPLE_SCHEDULER_H
#define SPARROW_SIMPLE_SCHEDULER_H

#include <vector>
#include <string>
#include <list>
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "sparrow/scheduler.h"
#include "sparrow/subscription-manager.h"
#include "sparrow/util.h"
#include "sparrow/state-store.h"
#include "util/metrics.h"
#include "gen-cpp/Types_types.h"  // for THostPort

namespace sparrow {

// Performs simple scheduling by matching between a list of hosts configured
// either from the state-store, or from a static list of addresses, and a list
// of target data locations.
class SimpleScheduler : public Scheduler {
 public:
  // Initialize with a subscription manager that we can register with for updates to the
  // set of available backends.
  SimpleScheduler(SubscriptionManager* subscription_manager,
      const ServiceId& backend_service_id, impala::Metrics* metrics);

  // Initialize with a list of <host:port> pairs in 'static' mode - i.e. the set of
  // backends is fixed and will not be updated.
  SimpleScheduler(const std::vector<impala::THostPort>& backends, 
      impala::Metrics* metrics);
  
  virtual ~SimpleScheduler();

  // Returns a list of backends such that the impalad at hostports[i] should be used to
  // read data from data_locations[i].
  // For each data_location, we choose a backend whose host matches the data_location in
  // a round robin fashion and insert it into hostports.
  // If no match is found for a data location, assign the data location in round-robin
  // order to any of the backends.
  // If the set of available hosts is updated between calls, round-robin state is reset.
  virtual impala::Status GetHosts(const HostList& data_locations, HostList* hostports);

  virtual void GetAllKnownHosts(HostList* hostports);

  // Registers with the subscription manager if required
  virtual impala::Status Init();

  // Unregister with the subscription manager
  virtual void Close();

 private:
  // map from host name to list of ports on which ImpalaServiceBackends
  // are listening
  typedef boost::unordered_map<std::string, std::list<int> > HostMap;
  HostMap host_map_;

  // Metrics subsystem access
  impala::Metrics* metrics_;

  // Protects access to host_map_, which may be updated asynchronously with respect to
  // reads. Also protects the locality counters, which are updated in GetHosts.
  boost::mutex host_map_lock_;

  // round robin entry in HostMap for non-local host assignment
  HostMap::iterator next_nonlocal_host_entry_;

  // Pointer to a subscription manager (which we do not own) which is used to register
  // for dynamic updates to the set of available backends. May be NULL if the set of
  // backends is fixed.
  SubscriptionManager* subscription_manager_;

  // UpdateCallback to use for registering a subscription with the subscription manager.
  SubscriptionManager::UpdateCallback callback_;

  // Subscription handle, used to unregister with subscription manager
  SubscriptionId subscription_id_;

  // Service identifier to subscribe to for backend membership information
  ServiceId backend_service_id_;

  // Locality metrics
  impala::Metrics::IntMetric* total_assignments_;
  impala::Metrics::IntMetric* total_local_assignments_;

  // Initialisation metric
  impala::Metrics::BooleanMetric* initialised_;

  // Called asynchronously when an update is received from the subscription manager
  void UpdateMembership(const ServiceStateMap& service_state);
};

}

#endif
