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

#ifndef STATESTORE_QUERY_SCHEDULE_H
#define STATESTORE_QUERY_SCHEDULE_H

#include <vector>
#include <string>
#include <boost/unordered_set.hpp>
#include <boost/unordered_map.hpp>

#include "common/global-types.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"  // for TNetworkAddress
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ResourceBrokerService_types.h"

namespace impala {

class Coordinator;

// map from scan node id to a list of scan ranges
typedef std::map<TPlanNodeId, std::vector<TScanRangeParams> > PerNodeScanRanges;
// map from an impalad host address to the per-node assigned scan ranges;
// records scan range assignment for a single fragment
typedef boost::unordered_map<TNetworkAddress, PerNodeScanRanges>
    FragmentScanRangeAssignment;

// execution parameters for a single fragment; used to assemble the
// per-fragment instance TPlanFragmentExecParams;
// hosts.size() == instance_ids.size()
struct FragmentExecParams {
  std::vector<TNetworkAddress> hosts; // execution backends
  std::vector<TUniqueId> instance_ids;
  std::vector<TPlanFragmentDestination> destinations;
  std::map<PlanNodeId, int> per_exch_num_senders;
  FragmentScanRangeAssignment scan_range_assignment;
};

// A QuerySchedule contains all necessary information for a query coordinator to
// generate fragment execution requests and start query execution. If resource management
// is enabled, then a schedule also contains the resource reservation request
// and the granted resource reservation.
// TODO: Consider moving QuerySchedule and all Schedulers into
// their own lib (and out of statestore).
class QuerySchedule {
 public:
  QuerySchedule(const TUniqueId& query_id, const TQueryExecRequest& request,
      const TQueryOptions& query_options, bool is_mini_llama);

  // Creates a reservation request for the given pool in reservation_request.
  // The request contains one resource per entry in unique_hosts_. The per-host resources
  // are based on planner estimates in the exec request or on manual overrides given
  // set in the query options.
  void CreateReservationRequest(const std::string& pool,
      const std::vector<std::string>& llama_nodes,
      TResourceBrokerReservationRequest* reservation_request);

  // Returns OK if reservation_ contains a matching resource for each
  // of the hosts in fragment_exec_params_. Returns an error otherwise.
  Status ValidateReservation();

  // Sets dest.hostname to the IP address corresponding to src.hostname
  // (if not already an IP address). Copies src.port to dest.port.
  // This method is needed because Llama expects the locations of reservation requests
  // to be IP addresses, but the Impala's scheduler assigns scan ranges to hostnames
  // (i.e., unique_hosts_ contains hostnames).
  void GetIpAddress(const TNetworkAddress& src, TNetworkAddress* dest);

  const TUniqueId& query_id() const { return query_id_; }
  const TQueryExecRequest& request() const { return request_; }
  const TQueryOptions& query_options() const { return query_options_; }
  bool HasReservation() const { return !reservation_.allocated_resources.empty(); }
  bool is_mini_llama() const { return is_mini_llama_; }
  const TNetworkAddress& impalad_to_dn(const TNetworkAddress& impalad_hostport) {
    return impalad_to_dn_[impalad_hostport];
  }
  const TNetworkAddress& dn_to_impalad(const TNetworkAddress& dn_hostport) {
    return dn_to_impalad_[dn_hostport];
  }

  // Helper methods used by scheduler to populate this QuerySchedule.
  void AddScanRanges(int64_t delta) { num_scan_ranges_ += delta; }
  void SetNumBackends(int64_t num_backends) { num_backends_ = num_backends; }
  int64_t num_backends() const { return num_backends_; }
  int64_t num_scan_ranges() const { return num_scan_ranges_; }
  int32_t GetFragmentIdx(PlanNodeId id) const { return plan_node_to_fragment_idx_[id]; }
  std::vector<FragmentExecParams>& exec_params() { return fragment_exec_params_; }
  boost::unordered_set<TNetworkAddress>& unique_hosts() { return unique_hosts_; }
  TResourceBrokerReservationResponse* reservation() { return &reservation_; }

 private:
  // Populates the bi-directional hostport mapping for the Mini Llama based on
  // the given llama_nodes and the unique_hosts_ of this schedule.
  void CreateMiniLlamaMapping(const std::vector<std::string>& llama_nodes);

  // These references are valid for the lifetime of this query schedule because they
  // are all owned by the enclosing QueryExecState.
  const TUniqueId& query_id_;
  const TQueryExecRequest& request_;
  const TQueryOptions& query_options_;
  bool is_mini_llama_;

  // Impala mini clusters using the Mini Llama require translating the impalad hostports
  // to Hadoop DN hostports registered with the Llama during resource requests
  // (and then in reverse for translating granted resources to impalads).
  // These maps form a bi-directional hostport mapping Hadoop DN <-> impalad.
  boost::unordered_map<TNetworkAddress, TNetworkAddress> impalad_to_dn_;
  boost::unordered_map<TNetworkAddress, TNetworkAddress> dn_to_impalad_;

  // Maps from plan node id to its fragment index. Filled in c'tor.
  std::vector<int32_t> plan_node_to_fragment_idx_;

  // vector is indexed by fragment index from TQueryExecRequest.fragments;
  // populated by Scheduler::Schedule()
  std::vector<FragmentExecParams> fragment_exec_params_;

  // The set of hosts that the query will run on excluding the coordinator.
  boost::unordered_set<TNetworkAddress> unique_hosts_;

  // Number of backends executing plan fragments on behalf of this query.
  int64_t num_backends_;

  // Total number of scan ranges of this query.
  int64_t num_scan_ranges_;

  // Fulfilled reservation request. Populated by scheduler.
  TResourceBrokerReservationResponse reservation_;
};

}

#endif
