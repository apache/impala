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

#ifndef SCHEDULING_QUERY_SCHEDULE_H
#define SCHEDULING_QUERY_SCHEDULE_H

#include <vector>
#include <string>
#include <boost/unordered_set.hpp>
#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>

#include "common/global-types.h"
#include "common/status.h"
#include "scheduling/query-resource-mgr.h"
#include "util/promise.h"
#include "util/runtime-profile.h"
#include "gen-cpp/Types_types.h"  // for TNetworkAddress
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ResourceBrokerService_types.h"

namespace impala {

class Coordinator;

/// map from scan node id to a list of scan ranges
typedef std::map<TPlanNodeId, std::vector<TScanRangeParams> > PerNodeScanRanges;
/// map from an impalad host address to the per-node assigned scan ranges;
/// records scan range assignment for a single fragment
typedef boost::unordered_map<TNetworkAddress, PerNodeScanRanges>
    FragmentScanRangeAssignment;

/// execution parameters for a single fragment; used to assemble the
/// per-fragment instance TPlanFragmentExecParams;
/// hosts.size() == instance_ids.size()
struct FragmentExecParams {
  std::vector<TNetworkAddress> hosts; // execution backends
  std::vector<TUniqueId> instance_ids;
  std::vector<TPlanFragmentDestination> destinations;
  std::map<PlanNodeId, int> per_exch_num_senders;
  FragmentScanRangeAssignment scan_range_assignment;
  /// In its role as a data sender, a fragment instance is assigned a "sender id" to
  /// uniquely identify it to a receiver. The id that a particular fragment instance
  /// is assigned ranges from [sender_id_base, sender_id_base + N - 1], where
  /// N = hosts.size (i.e. N = number of fragment instances)
  int sender_id_base;
};

/// A QuerySchedule contains all necessary information for a query coordinator to
/// generate fragment execution requests and start query execution. If resource management
/// is enabled, then a schedule also contains the resource reservation request
/// and the granted resource reservation.
/// TODO: Consider moving QuerySchedule and all Schedulers into
/// their own lib (and out of statestore).
/// TODO: Move all global state (e.g. profiles) to QueryExecState (after it is decoupled
/// from ImpalaServer)
class QuerySchedule {
 public:
  QuerySchedule(const TUniqueId& query_id, const TQueryExecRequest& request,
      const TQueryOptions& query_options, RuntimeProfile* summary_profile,
      RuntimeProfile::EventSequence* query_events);

  /// Returns OK if reservation_ contains a matching resource for each
  /// of the hosts in fragment_exec_params_. Returns an error otherwise.
  Status ValidateReservation();

  const TUniqueId& query_id() const { return query_id_; }
  const TQueryExecRequest& request() const { return request_; }
  const TQueryOptions& query_options() const { return query_options_; }
  const std::string& request_pool() const { return request_pool_; }
  void set_request_pool(const std::string& pool_name) { request_pool_ = pool_name; }
  bool HasReservation() const { return !reservation_.allocated_resources.empty(); }

  /// Granted or timed out reservations need to be released. In both such cases,
  /// the reservation_'s reservation_id is set.
  bool NeedsRelease() const { return reservation_.__isset.reservation_id; }

  /// Gets the estimated memory (bytes) and vcores per-node. Returns the user specified
  /// estimate (MEM_LIMIT query parameter) if provided or the estimate from planning if
  /// available, but is capped at the amount of physical memory to avoid problems if
  /// either estimate is unreasonably large.
  int64_t GetPerHostMemoryEstimate() const;
  int16_t GetPerHostVCores() const;
  /// Total estimated memory for all nodes. set_num_hosts() must be set before calling.
  int64_t GetClusterMemoryEstimate() const;
  void GetResourceHostport(const TNetworkAddress& src, TNetworkAddress* dst);

  /// Helper methods used by scheduler to populate this QuerySchedule.
  void AddScanRanges(int64_t delta) { num_scan_ranges_ += delta; }
  void set_num_fragment_instances(int64_t num_fragment_instances) {
    num_fragment_instances_ = num_fragment_instances;
  }
  void set_num_hosts(int64_t num_hosts) {
    DCHECK_GT(num_hosts, 0);
    num_hosts_ = num_hosts;
  }
  int64_t num_fragment_instances() const { return num_fragment_instances_; }
  int64_t num_hosts() const { return num_hosts_; }
  int64_t num_scan_ranges() const { return num_scan_ranges_; }

  /// Map node ids to the index of their fragment in TQueryExecRequest.fragments.
  int32_t GetFragmentIdx(PlanNodeId id) const { return plan_node_to_fragment_idx_[id]; }

  /// Map node ids to the index of the node inside their plan.nodes list.
  int32_t GetNodeIdx(PlanNodeId id) const { return plan_node_to_plan_node_idx_[id]; }
  std::vector<FragmentExecParams>* exec_params() { return &fragment_exec_params_; }
  const boost::unordered_set<TNetworkAddress>& unique_hosts() const {
    return unique_hosts_;
  }
  TResourceBrokerReservationResponse* reservation() { return &reservation_; }
  const TResourceBrokerReservationRequest& reservation_request() const {
    return reservation_request_;
  }
  bool is_admitted() const { return is_admitted_; }
  void set_is_admitted(bool is_admitted) { is_admitted_ = is_admitted; }
  RuntimeProfile* summary_profile() { return summary_profile_; }
  RuntimeProfile::EventSequence* query_events() { return query_events_; }

  void SetUniqueHosts(const boost::unordered_set<TNetworkAddress>& unique_hosts);

  /// Populates reservation_request_ ready to submit a query to Llama for all initial
  /// resources required for this query.
  void PrepareReservationRequest(const std::string& pool, const std::string& user);

 private:

  /// These references are valid for the lifetime of this query schedule because they
  /// are all owned by the enclosing QueryExecState.
  const TUniqueId& query_id_;
  const TQueryExecRequest& request_;

  /// The query options from the TClientRequest
  const TQueryOptions& query_options_;
  RuntimeProfile* summary_profile_;
  RuntimeProfile::EventSequence* query_events_;

  /// Maps from plan node id to its fragment index. Filled in c'tor.
  std::vector<int32_t> plan_node_to_fragment_idx_;

  /// Maps from plan node id to its index in plan.nodes. Filled in c'tor.
  std::vector<int32_t> plan_node_to_plan_node_idx_;

  /// vector is indexed by fragment index from TQueryExecRequest.fragments;
  /// populated by Scheduler::Schedule()
  std::vector<FragmentExecParams> fragment_exec_params_;

  /// The set of hosts that the query will run on excluding the coordinator.
  boost::unordered_set<TNetworkAddress> unique_hosts_;

  /// Number of backends executing plan fragments on behalf of this query.
  int64_t num_fragment_instances_;

  /// Total number of hosts. Used to compute the total cluster estimated memory
  /// in GetClusterMemoryEstimate().
  int64_t num_hosts_;

  /// Total number of scan ranges of this query.
  int64_t num_scan_ranges_;

  /// Request pool to which the request was submitted for admission.
  std::string request_pool_;

  /// Reservation request to be submitted to Llama. Set in PrepareReservationRequest().
  TResourceBrokerReservationRequest reservation_request_;

  /// Fulfilled reservation request. Populated by scheduler.
  TResourceBrokerReservationResponse reservation_;

  /// Indicates if the query has been admitted for execution.
  bool is_admitted_;

  /// Resolves unique_hosts_ to node mgr addresses. Valid only after SetUniqueHosts() has
  /// been called.
  boost::scoped_ptr<ResourceResolver> resource_resolver_;
};

}

#endif
