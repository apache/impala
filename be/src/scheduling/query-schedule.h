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
struct FInstanceExecParams;

/// map from scan node id to a list of scan ranges
typedef std::map<TPlanNodeId, std::vector<TScanRangeParams>> PerNodeScanRanges;

/// map from an impalad host address to the per-node assigned scan ranges;
/// records scan range assignment for a single fragment
typedef std::unordered_map<TNetworkAddress, PerNodeScanRanges>
    FragmentScanRangeAssignment;

/// Execution parameters for a single backend. Computed by Scheduler::Schedule(), set
/// via QuerySchedule::set_per_backend_exec_params(). Used as an input to
/// AdmissionController and a BackendState.
struct BackendExecParams {
  /// The fragment instance params assigned to this backend. All instances of a
  /// particular fragment are contiguous in this vector. Query lifetime;
  /// FInstanceExecParams are owned by QuerySchedule::fragment_exec_params_.
  std::vector<const FInstanceExecParams*> instance_params;

  // The minimum query-wide buffer reservation size (in bytes) required for this backend.
  // This is the peak minimum reservation that may be required by the
  // concurrently-executing operators at any point in query execution. It may be less
  // than the initial reservation total claims (below) if execution of some operators
  // never overlaps, which allows reuse of reservations.
  int64_t min_mem_reservation_bytes = 0;

  // Total of the initial buffer reservations that we expect to be claimed on this
  // backend for all fragment instances in instance_params. I.e. the sum over all
  // operators in all fragment instances that execute on this backend. This is used for
  // an optimization in InitialReservation. Measured in bytes.
  int64_t initial_mem_reservation_total_claims = 0;

  // Total thread reservation for fragment instances scheduled on this backend. This is
  // the peak number of required threads that may be required by the
  // concurrently-executing fragment instances at any point in query execution.
  int64_t thread_reservation = 0;

  // The maximum bytes of memory that can be admitted to this backend by the
  // admission controller. Obtained from the scheduler's executors configuration
  // which is updated by membership updates from the statestore.
  int64_t admit_mem_limit = 0;
};

/// Map from an impalad host address to the list of assigned fragment instance params.
typedef std::unordered_map<TNetworkAddress, BackendExecParams> PerBackendExecParams;

/// Execution parameters for a single fragment instance; used to assemble the
/// TPlanFragmentInstanceCtx
struct FInstanceExecParams {
  TUniqueId instance_id;
  TNetworkAddress host; // Thrift address of execution backend.
  TNetworkAddress krpc_host; // Krpc address of execution backend.
  PerNodeScanRanges per_node_scan_ranges;

  /// 0-based ordinal of this particular instance within its fragment (not: query-wide)
  int per_fragment_instance_idx;

  /// In its role as a data sender, a fragment instance is assigned a "sender id" to
  /// uniquely identify it to a receiver. -1 = invalid.
  int sender_id;

  /// The parent FragmentExecParams
  const FragmentExecParams& fragment_exec_params;
  const TPlanFragment& fragment() const;

  FInstanceExecParams(const TUniqueId& instance_id, const TNetworkAddress& host,
      const TNetworkAddress& krpc_host, int per_fragment_instance_idx,
      const FragmentExecParams& fragment_exec_params)
    : instance_id(instance_id),
      host(host),
      krpc_host(krpc_host),
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
/// scheduling logic itself.
/// The general usage pattern is that part of its state gets set from the static
/// TQueryExecRequest during initialization, then the actual schedule gets set by the
/// scheduler, then finally it is passed to the admission controller that keeps updating
/// the memory requirements by calling UpdateMemoryRequirements() every time it tries to
/// admit the query but only sets the final values once the query gets admitted
/// successfully. Note: Due to this usage pattern the memory requirement values should not
/// be accessed by other clients of this class while the query is in admission control
/// phase.
class QuerySchedule {
 public:
  QuerySchedule(const TUniqueId& query_id, const TQueryExecRequest& request,
      const TQueryOptions& query_options, RuntimeProfile* summary_profile,
      RuntimeProfile::EventSequence* query_events);

  /// For testing only: build a QuerySchedule object but do not run Init().
  QuerySchedule(const TUniqueId& query_id, const TQueryExecRequest& request,
      const TQueryOptions& query_options, RuntimeProfile* summary_profile);

  /// Verifies that the schedule is well-formed (and DCHECKs if it isn't):
  /// - all fragments have a FragmentExecParams
  /// - all scan ranges are assigned
  void Validate() const;

  const TUniqueId& query_id() const { return query_id_; }
  const TQueryExecRequest& request() const { return request_; }
  const TQueryOptions& query_options() const { return query_options_; }

  // Valid after Schedule() succeeds.
  const std::string& request_pool() const { return request().query_ctx.request_pool; }

  /// Returns the estimated memory (bytes) per-node from planning.
  int64_t GetPerHostMemoryEstimate() const;

  /// Helper methods used by scheduler to populate this QuerySchedule.
  void IncNumScanRanges(int64_t delta) { num_scan_ranges_ += delta; }

  /// Return the coordinator fragment, or nullptr if there isn't one.
  const TPlanFragment* GetCoordFragment() const;

  /// Return all fragments belonging to exec request in 'fragments'.
  void GetTPlanFragments(std::vector<const TPlanFragment*>* fragments) const;

  int64_t num_scan_ranges() const { return num_scan_ranges_; }

  /// Map node ids to the id of their containing fragment.
  FragmentIdx GetFragmentIdx(PlanNodeId id) const {
    return plan_node_to_fragment_idx_[id];
  }

  /// Return the total number of instances across all fragments.
  int GetNumFragmentInstances() const;

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

  const PerBackendExecParams& per_backend_exec_params() const {
    return per_backend_exec_params_;
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

  RuntimeProfile* summary_profile() { return summary_profile_; }
  RuntimeProfile::EventSequence* query_events() { return query_events_; }

  int64_t largest_min_reservation() const { return largest_min_reservation_; }

  /// Must call UpdateMemoryRequirements() at least once before calling this.
  int64_t per_backend_mem_limit() const { return per_backend_mem_limit_; }

  /// Must call UpdateMemoryRequirements() at least once before calling this.
  int64_t per_backend_mem_to_admit() const {
    DCHECK_GT(per_backend_mem_to_admit_, 0);
    return per_backend_mem_to_admit_;
  }

  void set_per_backend_exec_params(const PerBackendExecParams& params) {
    per_backend_exec_params_ = params;
  }

  void set_largest_min_reservation(const int64_t largest_min_reservation) {
    largest_min_reservation_ = largest_min_reservation;
  }

  /// Returns the Cluster wide memory admitted by the admission controller.
  /// Must call UpdateMemoryRequirements() at least once before calling this.
  int64_t GetClusterMemoryToAdmit() const;

  /// Populates or updates the per host query memory limit and the amount of memory to be
  /// admitted based on the pool configuration passed to it. Must be called at least once
  /// before making any calls to per_backend_mem_to_admit(), per_backend_mem_limit() and
  /// GetClusterMemoryToAdmit().
  void UpdateMemoryRequirements(const TPoolConfig& pool_cfg);

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

  // Map of host address to list of assigned FInstanceExecParams*, which
  // reference fragment_exec_params_. Computed in Scheduler::Schedule().
  PerBackendExecParams per_backend_exec_params_;

  /// Total number of scan ranges of this query.
  int64_t num_scan_ranges_;

  /// Used to generate consecutive fragment instance ids.
  TUniqueId next_instance_id_;

  /// The largest min memory reservation across all backends. Set in
  /// Scheduler::Schedule().
  int64_t largest_min_reservation_ = 0;

  /// The memory limit per backend that will be imposed on the query.
  /// Set by the admission controller with a value that is only valid if it was admitted
  /// successfully. -1 means no limit.
  int64_t per_backend_mem_limit_ = 0;

  /// The per backend memory used for admission accounting.
  /// Set by the admission controller with a value that is only valid if it was admitted
  /// successfully.
  int64_t per_backend_mem_to_admit_ = 0;

  /// Populate fragment_exec_params_ from request_.plan_exec_info.
  /// Sets is_coord_fragment and input_fragments.
  /// Also populates plan_node_to_fragment_idx_ and plan_node_to_plan_node_idx_.
  void Init();
};

}

#endif
