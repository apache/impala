// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef SCHEDULING_QUERY_SCHEDULE_H
#define SCHEDULING_QUERY_SCHEDULE_H

#include <vector>
#include <string>
#include <unordered_map>
#include <boost/unordered_set.hpp>
#include <boost/scoped_ptr.hpp>

#include "common/global-types.h"
#include "common/status.h"
#include "util/promise.h"
#include "util/container-util.h"
#include "util/runtime-profile.h"
#include "gen-cpp/Types_types.h"  // for TNetworkAddress
#include "gen-cpp/Frontend_types.h"

namespace impala {

class Coordinator;
struct FragmentExecParams;

/// map from scan node id to a list of scan ranges
typedef std::map<TPlanNodeId, std::vector<TScanRangeParams>> PerNodeScanRanges;

/// map from an impalad host address to the per-node assigned scan ranges;
/// records scan range assignment for a single fragment
typedef std::unordered_map<TNetworkAddress, PerNodeScanRanges>
    FragmentScanRangeAssignment;

/// execution parameters for a single fragment instance; used to assemble the
/// TPlanFragmentInstanceCtx
struct FInstanceExecParams {
  TUniqueId instance_id;
  TNetworkAddress host; // execution backend
  PerNodeScanRanges per_node_scan_ranges;

  /// 0-based ordinal of this particular instance within its fragment (not: query-wide)
  int per_fragment_instance_idx;

  /// In its role as a data sender, a fragment instance is assigned a "sender id" to
  /// uniquely identify it to a receiver. -1 = invalid.
  int sender_id;

  /// the parent FragmentExecParams
  const FragmentExecParams& fragment_exec_params;
  const TPlanFragment& fragment() const;

  FInstanceExecParams(const TUniqueId& instance_id, const TNetworkAddress& host,
      int per_fragment_instance_idx, const FragmentExecParams& fragment_exec_params)
    : instance_id(instance_id), host(host),
      per_fragment_instance_idx(per_fragment_instance_idx),
      sender_id(-1),
      fragment_exec_params(fragment_exec_params) {}
};

/// Execution parameters shared between fragment instances
struct FragmentExecParams {
  /// output destinations of this fragment
  std::vector<TPlanFragmentDestination> destinations;

  /// map from node id to the number of senders (node id expected to be for an
  /// ExchangeNode)
  std::map<PlanNodeId, int> per_exch_num_senders;

  // only needed as intermediate state during exec parameter computation;
  // for scheduling, refer to FInstanceExecParams.per_node_scan_ranges
  FragmentScanRangeAssignment scan_range_assignment;

  bool is_coord_fragment;
  const TPlanFragment& fragment;
  std::vector<FragmentIdx> input_fragments;
  std::vector<FInstanceExecParams> instance_exec_params;

  FragmentExecParams(const TPlanFragment& fragment)
    : is_coord_fragment(false), fragment(fragment) {}

  // extract instance indices from instance_exec_params.instance_id
  std::vector<int> GetInstanceIdxs() const;
};

/// A QuerySchedule contains all necessary information for a query coordinator to
/// generate fragment execution requests and start query execution. If resource management
/// is enabled, then a schedule also contains the resource reservation request
/// and the granted resource reservation.
///
/// QuerySchedule is a container class for scheduling data, but it doesn't contain
/// scheduling logic itself. Its state either comes from the static TQueryExecRequest
/// or is computed by Scheduler.
class QuerySchedule {
 public:
  QuerySchedule(const TUniqueId& query_id, const TQueryExecRequest& request,
      const TQueryOptions& query_options, RuntimeProfile* summary_profile,
      RuntimeProfile::EventSequence* query_events);

  /// Verifies that the schedule is well-formed (and DCHECKs if it isn't):
  /// - all fragments have a FragmentExecParams
  /// - all scan ranges are assigned
  void Validate() const;

  const TUniqueId& query_id() const { return query_id_; }
  const TQueryExecRequest& request() const { return request_; }
  const TQueryOptions& query_options() const { return query_options_; }
  const std::string& request_pool() const { return request_pool_; }
  void set_request_pool(const std::string& pool_name) { request_pool_ = pool_name; }

  /// Gets the estimated memory (bytes) per-node. Returns the user specified estimate
  /// (MEM_LIMIT query parameter) if provided or the estimate from planning if available,
  /// but is capped at the amount of physical memory to avoid problems if either estimate
  /// is unreasonably large.
  int64_t GetPerHostMemoryEstimate() const;
  /// Total estimated memory for all nodes. set_num_hosts() must be set before calling.
  int64_t GetClusterMemoryEstimate() const;

  /// Helper methods used by scheduler to populate this QuerySchedule.
  void IncNumScanRanges(int64_t delta) { num_scan_ranges_ += delta; }

  /// Returns the total number of fragment instances.
  int GetNumFragmentInstances() const;

  /// Return the coordinator fragment, or nullptr if there isn't one.
  const TPlanFragment* GetCoordFragment() const;

  /// Return all fragments belonging to exec request in 'fragments'.
  void GetTPlanFragments(std::vector<const TPlanFragment*>* fragments) const;

  int64_t num_scan_ranges() const { return num_scan_ranges_; }

  /// Map node ids to the id of their containing fragment.
  FragmentIdx GetFragmentIdx(PlanNodeId id) const {
    return plan_node_to_fragment_idx_[id];
  }

  /// Returns next instance id. Instance ids are consecutive numbers generated from
  /// the query id.
  /// If the query contains a coordinator fragment instance, the generated instance
  /// ids start at 1 and the caller is responsible for assigning the correct id
  /// to the coordinator instance. If the query does not contain a coordinator instance,
  /// the generated instance ids start at 0.
  TUniqueId GetNextInstanceId();

  const TPlanFragment& GetContainingFragment(PlanNodeId node_id) const {
    FragmentIdx fragment_idx = GetFragmentIdx(node_id);
    DCHECK_LT(fragment_idx, fragment_exec_params_.size());
    return fragment_exec_params_[fragment_idx].fragment;
  }

  const TPlanNode& GetNode(PlanNodeId id) const {
    const TPlanFragment& fragment = GetContainingFragment(id);
    return fragment.plan.nodes[plan_node_to_plan_node_idx_[id]];
  }

  const std::vector<FragmentExecParams>& fragment_exec_params() const {
    return fragment_exec_params_;
  }
  const FragmentExecParams& GetFragmentExecParams(FragmentIdx idx) const {
    return fragment_exec_params_[idx];
  }
  FragmentExecParams* GetFragmentExecParams(FragmentIdx idx) {
    return &fragment_exec_params_[idx];
  }

  const FInstanceExecParams& GetCoordInstanceExecParams() const;

  const boost::unordered_set<TNetworkAddress>& unique_hosts() const {
    return unique_hosts_;
  }
  bool is_admitted() const { return is_admitted_; }
  void set_is_admitted(bool is_admitted) { is_admitted_ = is_admitted; }
  RuntimeProfile* summary_profile() { return summary_profile_; }
  RuntimeProfile::EventSequence* query_events() { return query_events_; }

  void SetUniqueHosts(const boost::unordered_set<TNetworkAddress>& unique_hosts);

 private:
  /// These references are valid for the lifetime of this query schedule because they
  /// are all owned by the enclosing QueryExecState.
  const TUniqueId& query_id_;
  const TQueryExecRequest& request_;

  /// The query options from the TClientRequest
  const TQueryOptions& query_options_;

  /// TODO: move these into QueryState
  RuntimeProfile* summary_profile_;
  RuntimeProfile::EventSequence* query_events_;

  /// Maps from plan node id to its fragment idx. Filled in c'tor.
  std::vector<int32_t> plan_node_to_fragment_idx_;

  /// Maps from plan node id to its index in plan.nodes. Filled in c'tor.
  std::vector<int32_t> plan_node_to_plan_node_idx_;

  // populated in Init() and Scheduler::Schedule()
  // (Scheduler::ComputeFInstanceExecParams()), indexed by fragment idx
  // (TPlanFragment.idx)
  std::vector<FragmentExecParams> fragment_exec_params_;

  /// The set of hosts that the query will run on excluding the coordinator.
  boost::unordered_set<TNetworkAddress> unique_hosts_;

  /// Total number of scan ranges of this query.
  int64_t num_scan_ranges_;

  /// Used to generate consecutive fragment instance ids.
  TUniqueId next_instance_id_;

  /// Request pool to which the request was submitted for admission.
  std::string request_pool_;

  /// Indicates if the query has been admitted for execution.
  bool is_admitted_;

  /// Populate fragment_exec_params_ from request_.plan_exec_info.
  /// Sets is_coord_fragment and input_fragments.
  /// Also populates plan_node_to_fragment_idx_ and plan_node_to_plan_node_idx_.
  void Init();
};

}

#endif
