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


#ifndef SCHEDULING_SIMPLE_SCHEDULER_H
#define SCHEDULING_SIMPLE_SCHEDULER_H

#include <vector>
#include <string>
#include <list>
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "scheduling/scheduler.h"
#include "statestore/statestore-subscriber.h"
#include "statestore/statestore.h"
#include "util/metrics.h"
#include "scheduling/admission-controller.h"
#include "gen-cpp/Types_types.h"  // for TNetworkAddress
#include "gen-cpp/ResourceBrokerService_types.h"
#include "rapidjson/rapidjson.h"

namespace impala {

class ResourceBroker;
class Coordinator;

/// Performs simple scheduling by matching between a list of backends configured
/// either from the statestore, or from a static list of addresses, and a list
/// of target data locations.
//
/// TODO: Notice when there are duplicate statestore registrations (IMPALA-23)
/// TODO: Handle deltas from the statestore
class SimpleScheduler : public Scheduler {
 public:
  static const std::string IMPALA_MEMBERSHIP_TOPIC;

  /// Initialize with a subscription manager that we can register with for updates to the
  /// set of available backends.
  ///  - backend_id - unique identifier for this Impala backend (usually a host:port)
  ///  - backend_address - the address that this backend listens on
  SimpleScheduler(StatestoreSubscriber* subscriber, const std::string& backend_id,
      const TNetworkAddress& backend_address, MetricGroup* metrics, Webserver* webserver,
      ResourceBroker* resource_broker, RequestPoolService* request_pool_service);

  /// Initialize with a list of <host:port> pairs in 'static' mode - i.e. the set of
  /// backends is fixed and will not be updated.
  SimpleScheduler(const std::vector<TNetworkAddress>& backends, MetricGroup* metrics,
      Webserver* webserver, ResourceBroker* resource_broker,
      RequestPoolService* request_pool_service);

  /// Returns a list of backends such that the impalad at backends[i] should be used to
  /// read data from data_locations[i].
  /// For each data_location, we choose a backend whose host matches the data_location in
  /// a round robin fashion and insert it into backends.
  /// If no match is found for a data location, assign the data location in round-robin
  /// order to any of the backends.
  /// If the set of available backends is updated between calls, round-robin state is reset.
  virtual Status GetBackends(const std::vector<TNetworkAddress>& data_locations,
      BackendList* backends);

  /// Return a backend such that the impalad at backend.address should be used to read data
  /// from the given data_loation
  virtual impala::Status GetBackend(const TNetworkAddress& data_location,
      TBackendDescriptor* backend);

  virtual void GetAllKnownBackends(BackendList* backends);

  virtual bool HasLocalBackend(const TNetworkAddress& data_location) {
    boost::lock_guard<boost::mutex> l(backend_map_lock_);
    BackendMap::iterator entry = backend_map_.find(data_location.hostname);
    return (entry != backend_map_.end() && entry->second.size() > 0);
  }

  /// Registers with the subscription manager if required
  virtual impala::Status Init();

  virtual Status Schedule(Coordinator* coord, QuerySchedule* schedule);
  virtual Status Release(QuerySchedule* schedule);
  virtual void HandlePreemptedReservation(const TUniqueId& reservation_id);
  virtual void HandlePreemptedResource(const TUniqueId& client_resource_id);
  virtual void HandleLostResource(const TUniqueId& client_resource_id);

 private:
  /// Protects access to backend_map_ and backend_ip_map_, which might otherwise be updated
  /// asynchronously with respect to reads. Also protects the locality
  /// counters, which are updated in GetBackends.
  boost::mutex backend_map_lock_;

  /// Map from a datanode's IP address to a list of backend addresses running on that node.
  typedef boost::unordered_map<std::string, std::list<TBackendDescriptor> > BackendMap;
  BackendMap backend_map_;

  /// Map from a datanode's hostname to its IP address to support both hostname based
  /// lookup.
  typedef boost::unordered_map<std::string, std::string> BackendIpAddressMap;
  BackendIpAddressMap backend_ip_map_;

  /// Map from unique backend id to TBackendDescriptor. Used to track the known backends
  /// from the statestore. It's important to track both the backend ID as well as the
  /// TBackendDescriptor so we know what is being removed in a given update.
  /// Locking of this map is not needed since it should only be read/modified from
  /// within the UpdateMembership() function.
  typedef boost::unordered_map<std::string, TBackendDescriptor> BackendIdMap;
  BackendIdMap current_membership_;

  /// MetricGroup subsystem access
  MetricGroup* metrics_;

  /// Webserver for /backends. Not owned by us.
  Webserver* webserver_;

  /// round robin entry in BackendMap for non-local host assignment
  BackendMap::iterator next_nonlocal_backend_entry_;

  /// Pointer to a subscription manager (which we do not own) which is used to register
  /// for dynamic updates to the set of available backends. May be NULL if the set of
  /// backends is fixed.
  StatestoreSubscriber* statestore_subscriber_;

  /// Unique - across the cluster - identifier for this impala backend
  const std::string backend_id_;

  /// Describes this backend, including the Impalad service address
  TBackendDescriptor backend_descriptor_;

  ThriftSerializer thrift_serializer_;

  /// Locality metrics
  IntCounter* total_assignments_;
  IntCounter* total_local_assignments_;

  /// Initialisation metric
  BooleanProperty* initialised_;
  /// Current number of backends
  IntGauge* num_backends_metric_;

  /// Counts the number of UpdateMembership invocations, to help throttle the logging.
  uint32_t update_count_;

  /// Protects active_reservations_ and active_client_resources_.
  boost::mutex active_resources_lock_;

  /// Maps from a Llama reservation id to the coordinator of the query using that
  /// reservation.  The map is used to cancel queries whose reservation has been preempted.
  /// Entries are added in Schedule() calls that result in granted resource allocations.
  /// Entries are removed in Release().
  typedef boost::unordered_map<TUniqueId, Coordinator*> ActiveReservationsMap;
  ActiveReservationsMap active_reservations_;

  /// Maps from client resource id to the coordinator of the query using that resource.
  /// The map is used to cancel queries whose resource(s) have been preempted.
  /// Entries are added in Schedule() calls that result in granted resource allocations.
  /// Entries are removed in Release().
  typedef boost::unordered_map<TUniqueId, Coordinator*> ActiveClientResourcesMap;
  ActiveClientResourcesMap active_client_resources_;

  /// Resource broker that mediates resource requests between Impala and the Llama.
  /// Set to NULL if resource management is disabled.
  ResourceBroker* resource_broker_;

  /// Used for user-to-pool resolution and looking up pool configurations. Not owned by
  /// us.
  RequestPoolService* request_pool_service_;

  /// Used to make admission decisions in 'Schedule()'
  boost::scoped_ptr<AdmissionController> admission_controller_;

  /// Adds the granted reservation and resources to the active_reservations_ and
  /// active_client_resources_ maps, respectively.
  void AddToActiveResourceMaps(
      const TResourceBrokerReservationResponse& reservation, Coordinator* coord);

  /// Removes the given reservation and resources from the active_reservations_ and
  /// active_client_resources_ maps, respectively.
  void RemoveFromActiveResourceMaps(
      const TResourceBrokerReservationResponse& reservation);

  /// Called asynchronously when an update is received from the subscription manager
  void UpdateMembership(const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
      std::vector<TTopicDelta>* subscriber_topic_updates);

  /// Webserver callback that produces a list of known backends.
  /// Example output:
  /// "backends": [
  ///     "henry-metrics-pkg-cdh5.ent.cloudera.com:22000"
  ///              ],
  void BackendsUrlCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Determines the pool for a user and query options via request_pool_service_.
  Status GetRequestPool(const std::string& user,
      const TQueryOptions& query_options, std::string* pool) const;

  /// Computes the assignment of scan ranges to hosts for each scan node in schedule.
  /// Unpartitioned fragments are assigned to the coord. Populates the schedule's
  /// fragment_exec_params_ with the resulting scan range assignment.
  Status ComputeScanRangeAssignment(const TQueryExecRequest& exec_request,
      QuerySchedule* schedule);

  /// Does a scan range assignment (returned in 'assignment') based on a list of scan
  /// range locations for a particular scan node.
  /// If exec_at_coord is true, all scan ranges will be assigned to the coord node.
  Status ComputeScanRangeAssignment(PlanNodeId node_id,
      const std::vector<TScanRangeLocations>& locations,
      const std::vector<TNetworkAddress>& host_list, bool exec_at_coord,
      const TQueryOptions& query_options, FragmentScanRangeAssignment* assignment);

  /// Populates fragment_exec_params_ in schedule.
  void ComputeFragmentExecParams(const TQueryExecRequest& exec_request,
      QuerySchedule* schedule);

  /// For each fragment in exec_request, computes hosts on which to run the instances
  /// and stores result in fragment_exec_params_.hosts.
  void ComputeFragmentHosts(const TQueryExecRequest& exec_request,
      QuerySchedule* schedule);

  /// Returns the id of the leftmost node of any of the given types in 'plan',
  /// or INVALID_PLAN_NODE_ID if no such node present.
  PlanNodeId FindLeftmostNode(
      const TPlan& plan, const std::vector<TPlanNodeType::type>& types);

  /// Returns the index (w/in exec_request.fragments) of fragment that sends its output
  /// to exec_request.fragment[fragment_idx]'s leftmost ExchangeNode.
  /// Returns INVALID_PLAN_NODE_ID if the leftmost node is not an exchange node.
  int FindLeftmostInputFragment(
      int fragment_idx, const TQueryExecRequest& exec_request);

  /// Adds all hosts the given scan is executed on to scan_hosts.
  void GetScanHosts(TPlanNodeId scan_id, const TQueryExecRequest& exec_request,
      const FragmentExecParams& params, std::vector<TNetworkAddress>* scan_hosts);

  /// Returns true if 'plan' contains a node of the given type.
  bool ContainsNode(const TPlan& plan, TPlanNodeType::type type);

  /// Returns all ids of nodes in 'plan' of any of the given types.
  void FindNodes(const TPlan& plan,
      const std::vector<TPlanNodeType::type>& types, std::vector<TPlanNodeId>* results);

  /// Returns the index (w/in exec_request.fragments) of fragment that sends its output
  /// to the given exchange in the given fragment index.
  int FindSenderFragment(TPlanNodeId exch_id, int fragment_idx,
      const TQueryExecRequest& exec_request);
};

}

#endif
