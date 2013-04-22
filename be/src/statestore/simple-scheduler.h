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


#ifndef STATESTORE_SIMPLE_SCHEDULER_H
#define STATESTORE_SIMPLE_SCHEDULER_H

#include <vector>
#include <string>
#include <list>
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "statestore/scheduler.h"
#include "statestore/state-store-subscriber.h"
#include "statestore/state-store.h"
#include "util/metrics.h"
#include "gen-cpp/Types_types.h"  // for TNetworkAddress

namespace impala {

// Performs simple scheduling by matching between a list of hosts configured
// either from the state-store, or from a static list of addresses, and a list
// of target data locations.
//
// TODO: Notice when there are duplicate state-store registrations (IMPALA-23)
// TODO: Handle deltas from the state-store
class SimpleScheduler : public Scheduler {
 public:
  static const std::string IMPALA_MEMBERSHIP_TOPIC;

  // Initialize with a subscription manager that we can register with for updates to the
  // set of available backends.
  //  - backend_id - unique identifier for this Impala backend (usually a host:port)
  //  - backend_address - the address that this backend listens on
  SimpleScheduler(StateStoreSubscriber* subscriber, const std::string& backend_id,
      const TNetworkAddress& backend_address, Metrics* metrics);

  // Initialize with a list of <host:port> pairs in 'static' mode - i.e. the set of
  // backends is fixed and will not be updated.
  SimpleScheduler(const std::vector<TNetworkAddress>& backends, Metrics* metrics);

  // Returns a list of backends such that the impalad at hostports[i] should be used to
  // read data from data_locations[i].
  // For each data_location, we choose a backend whose host matches the data_location in
  // a round robin fashion and insert it into hostports.
  // If no match is found for a data location, assign the data location in round-robin
  // order to any of the backends.
  // If the set of available hosts is updated between calls, round-robin state is reset.
  virtual impala::Status GetHosts(const HostList& data_locations, HostList* hostports);

  // Return a backend such that the impalad at hostport should be used to read data
  // from the given data_loation
  virtual impala::Status GetHost(const TNetworkAddress& data_location,
      TNetworkAddress* hostport);

  virtual void GetAllKnownHosts(HostList* hostports);

  virtual bool HasLocalHost(const TNetworkAddress& data_location) {
    boost::lock_guard<boost::mutex> l(host_map_lock_);
    HostMap::iterator entry = host_map_.find(data_location.hostname);
    return (entry != host_map_.end() && entry->second.size() > 0);
  }

  // Registers with the subscription manager if required
  virtual impala::Status Init();

 private:
  // Protects access to host_map_ and host_ip_map_, which might otherwise be updated
  // asynchronously with respect to reads. Also protects the locality
  // counters, which are updated in GetHosts.
  boost::mutex host_map_lock_;

  // Map from a datanode's IP address to a list of backend addresses running on that node.
  typedef boost::unordered_map<std::string, std::list<TNetworkAddress> > HostMap;
  HostMap host_map_;

  // Map from a datanode's hostname to its IP address to support both hostname based
  // lookup.
  typedef boost::unordered_map<std::string, std::string> HostIpAddressMap;
  HostIpAddressMap host_ip_map_;

  // Metrics subsystem access
  impala::Metrics* metrics_;

  // round robin entry in HostMap for non-local host assignment
  HostMap::iterator next_nonlocal_host_entry_;

  // Pointer to a subscription manager (which we do not own) which is used to register
  // for dynamic updates to the set of available backends. May be NULL if the set of
  // backends is fixed.
  StateStoreSubscriber* statestore_subscriber_;

  // Unique - across the cluster - identifier for this impala backend
  const std::string backend_id_;

  // Describes this backend, including the Impalad service address
  TBackendDescriptor backend_descriptor_;

  ThriftSerializer thrift_serializer_;

  // Locality metrics
  Metrics::IntMetric* total_assignments_;
  Metrics::IntMetric* total_local_assignments_;

  // Initialisation metric
  Metrics::BooleanMetric* initialised_;

  // Counts the number of UpdateMembership invocations, to help throttle the logging.
  uint32_t update_count_;

  // Called asynchronously when an update is received from the subscription manager
  void UpdateMembership(const StateStoreSubscriber::TopicDeltaMap& service_state,
      std::vector<TTopicUpdate>* topic_updates);
};

}

#endif
