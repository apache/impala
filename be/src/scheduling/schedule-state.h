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

#pragma once

#include <map>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/global-types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/admission_control_service.pb.h"
#include "gen-cpp/statestore_service.pb.h"
#include "scheduling/executor-group.h"
#include "util/container-util.h"
#include "util/runtime-profile.h"

namespace impala {

struct FragmentScheduleState;
struct FInstanceScheduleState;

/// map from scan node id to a list of scan ranges
typedef std::map<TPlanNodeId, std::vector<ScanRangeParamsPB>> PerNodeScanRanges;

/// map from an impalad host address to the per-node assigned scan ranges;
/// records scan range assignment for a single fragment
typedef std::unordered_map<NetworkAddressPB, PerNodeScanRanges>
    FragmentScanRangeAssignment;

/// Execution parameters for a single backend. This gets created for every backend that
/// participates in query execution, which includes every backend that has fragments
/// scheduled on it and the coordinator backend.
///
/// Created by ScheduleState::GetOrCreateBackendScheduleState() and initialized in
/// Scheduler::ComputeBackendExecParams(). Used as an input to the
/// AdmissionController and Coordinator::BackendState.
struct BackendScheduleState {
  BackendDescriptorPB be_desc;

  /// Pointer to the corresponding protobuf struct containing any parameters for this
  /// backend that will need to be sent back to the coordinator. Owned by
  /// ScheduleState::query_schedule_pb_.
  BackendExecParamsPB* exec_params;

  explicit BackendScheduleState(BackendExecParamsPB* exec_params)
    : exec_params(exec_params) {}
};

/// Map from an impalad backend address to the state for that backend.
typedef std::unordered_map<NetworkAddressPB, BackendScheduleState>
    PerBackendScheduleStates;

/// Execution parameters for a single fragment instance. Contains both intermediate
/// info needed by the scheduler and info that will be sent back to the coordinator.
///
/// FInstanceScheduleStates are created as children of FragmentScheduleStates (in
/// 'instance_states') and then the calculated execution parameters, 'exec_params', are
/// transferred to the corresponding BackendExecParamsPB in
/// Scheduler::ComputeBackendExecParams().
struct FInstanceScheduleState {
  /// Address of execution backend: hostname + krpc_port.
  NetworkAddressPB host;

  /// Krpc address of execution backend: ip_address + krpc_port.
  NetworkAddressPB krpc_host;

  /// Contains any info that needs to be sent back to the coordinator. Computed during
  /// Scheduler::ComputeFragmentExecParams() then transferred to the corresponding
  /// BackendExecParamsPB in Scheduler::ComputeBackendExecParams(), after which it
  /// is no longer valid to access.
  FInstanceExecParamsPB exec_params;

  FInstanceScheduleState(const UniqueIdPB& instance_id, const NetworkAddressPB& host,
      const NetworkAddressPB& krpc_host, int per_fragment_instance_idx,
      const FragmentScheduleState& fragment_state);

  /// Adds the ranges in 'scan_ranges' to the scan at 'scan_idx' in 'exec_params'.
  void AddScanRanges(int scan_idx, const std::vector<ScanRangeParamsPB>& scan_ranges);
};

/// Execution parameters shared between fragment instances. This struct is a container for
/// any intermediate data needed for scheduling that will not be sent back to the
/// coordinator as part of the QuerySchedulePB along with a pointer to the corresponding
/// FragmentExecParamsPB in the QuerySchedulePB.
struct FragmentScheduleState {
  /// Only needed as intermediate state during exec parameter computation.
  /// For scheduling, refer to FInstanceExecParamsPB.per_node_scan_ranges
  FragmentScanRangeAssignment scan_range_assignment;

  /// The root fragment of the plan runs on the coordinator.
  bool is_root_coord_fragment;
  const TPlanFragment& fragment;

  /// Fragments that are inputs to an ExchangeNode of this fragment.
  std::vector<FragmentIdx> exchange_input_fragments;

  /// Instances of this fragment. Instances on a backend are clustered together - i.e. all
  /// instances for a given backend will be consecutive entries in the vector. These have
  /// their protobuf params Swap()-ed to the BackendExecParamsPB during
  /// Scheduler::ComputeBackendExecParams() and are no longer valid after that.
  std::vector<FInstanceScheduleState> instance_states;

  /// Pointer to the corresponding FragmentExecParamsPB in the parent ScheduleState's
  /// 'query_schedule_pb_'
  FragmentExecParamsPB* exec_params;

  FragmentScheduleState(const TPlanFragment& fragment, FragmentExecParamsPB* exec_params);
};

/// ScheduleState is a container class for scheduling data used by Scheduler and
/// AdmissionController, which perform the scheduling logic itself, and it is only
/// intended to be accessed by them. The information needed for the coordinator to begin
/// execution is stored in 'query_schedule_pb_', which is returned from the
/// AdmissionController on successful admission. Everything else is intermediate data
/// needed to calculate the schedule but is discarded after a scheduling decision is made.
///
/// The general usage pattern is:
/// - FragmentScheduleStates are created for each fragment in the plan. They are given
///   pointers to corresponding FragmentExecParamsPBs created in the QuerySchedulePB.
/// - FInstanceScheduleStates are created as children of the FragmentScheduleStates for
///   each finstance and assigned to hosts. The FInstanceScheduleStates each have a
///   corresponding FInstanceExecParamsPB that they initially own.
/// - The scheduler computes the BackendScheduleState for each backend that was assigned a
///   fragment instance (and the coordinator backend). They are given pointers to
///   corresponding BackendExecParamsPBs created in the QuerySchedulePB and the
///   FInstanceExecParamsPB are Swap()-ed into them.
/// - The ScheduleState is passed to the admission controller, which keeps updating the
///   memory requirements by calling UpdateMemoryRequirements() every time it tries to
///   admit the query and sets the final values once the query gets admitted successfully.
/// - On successful admission, the QuerySchedulePB is returned to the coordinator and
///   everything else is discarded.
class ScheduleState {
 public:
  /// For testing only: specify 'is_test=true' to build a ScheduleState object without
  /// running Init() and to seed the random number generator for deterministic results.
  ScheduleState(const UniqueIdPB& query_id, const TQueryExecRequest& request,
      const TQueryOptions& query_options, RuntimeProfile* summary_profile, bool is_test);

  /// Verifies that the schedule is well-formed (and DCHECKs if it isn't):
  /// - all fragments have a FragmentScheduleState
  /// - all scan ranges are assigned
  /// - all BackendScheduleStates have instances assigned except for coordinator.
  /// Expected to be called after the BackendScheduleStates have been computed, i.e. the
  /// FInstanceExecParamsPB will have already been swapped.
  void Validate() const;

  const UniqueIdPB& query_id() const { return query_id_; }
  const TQueryExecRequest& request() const { return request_; }
  const TQueryOptions& query_options() const { return query_options_; }

  std::unique_ptr<QuerySchedulePB>& query_schedule_pb() { return query_schedule_pb_; }

  /// Valid after Schedule() succeeds.
  const std::string& request_pool() const { return request().query_ctx.request_pool; }

  /// Returns the estimated memory (bytes) per-node from planning.
  int64_t GetPerExecutorMemoryEstimate() const;

  /// Returns the estimated memory (bytes) for the coordinator backend returned by the
  /// planner. This estimate is only meaningful if this schedule was generated on a
  /// dedicated coordinator.
  int64_t GetDedicatedCoordMemoryEstimate() const;

  /// Return whether the request is a trivial query.
  bool GetIsTrivialQuery() const;

  /// Helper methods used by scheduler to populate this ScheduleState.
  void IncNumScanRanges(int64_t delta);

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
  UniqueIdPB GetNextInstanceId();

  const TPlanFragment& GetContainingFragment(PlanNodeId node_id) const {
    FragmentIdx fragment_idx = GetFragmentIdx(node_id);
    DCHECK_LT(fragment_idx, fragment_schedule_states_.size());
    return fragment_schedule_states_[fragment_idx].fragment;
  }

  const TPlanNode& GetNode(PlanNodeId id) const {
    const TPlanFragment& fragment = GetContainingFragment(id);
    return fragment.plan.nodes[plan_node_to_plan_node_idx_[id]];
  }

  const PerBackendScheduleStates& per_backend_schedule_states() const {
    return per_backend_schedule_states_;
  }
  PerBackendScheduleStates& per_backend_schedule_states() {
    return per_backend_schedule_states_;
  }

  /// Returns a reference to the BackendScheduleState corresponding to 'address', creating
  /// it if it doesn't already exist.
  BackendScheduleState& GetOrCreateBackendScheduleState(const NetworkAddressPB& address);

  /// Removes any BackendScheduleStates that have been created. Only used in testing.
  void ClearBackendScheduleStates() {
    per_backend_schedule_states_.clear();
    query_schedule_pb_->clear_backend_exec_params();
  }

  std::vector<FragmentScheduleState>& fragment_schedule_states() {
    return fragment_schedule_states_;
  }
  const FragmentScheduleState& GetFragmentScheduleState(FragmentIdx idx) const {
    return fragment_schedule_states_[idx];
  }
  FragmentScheduleState* GetFragmentScheduleState(FragmentIdx idx) {
    return &fragment_schedule_states_[idx];
  }


  RuntimeProfile* summary_profile() { return summary_profile_; }

  int64_t largest_min_reservation() const { return largest_min_reservation_; }

  int64_t coord_min_reservation() const { return coord_min_reservation_; }

  /// Must call UpdateMemoryRequirements() at least once before calling this.
  int64_t per_backend_mem_limit() const {
    return query_schedule_pb_->per_backend_mem_limit();
  }

  /// Must call UpdateMemoryRequirements() at least once before calling this.
  int64_t per_backend_mem_to_admit() const {
    DCHECK_GE(query_schedule_pb_->per_backend_mem_to_admit(), 0);
    return query_schedule_pb_->per_backend_mem_to_admit();
  }

  /// Must call UpdateMemoryRequirements() at least once before calling this.
  MemLimitSourcePB per_backend_mem_to_admit_source() const {
    DCHECK(query_schedule_pb_->has_per_backend_mem_to_admit_source());
    return query_schedule_pb_->per_backend_mem_to_admit_source();
  }

  /// Must call UpdateMemoryRequirements() at least once before calling this.
  int64_t coord_backend_mem_limit() const {
    return query_schedule_pb_->coord_backend_mem_limit();
  }

  /// Must call UpdateMemoryRequirements() at least once before calling this.
  int64_t coord_backend_mem_to_admit() const {
    DCHECK_GT(query_schedule_pb_->coord_backend_mem_to_admit(), 0);
    return query_schedule_pb_->coord_backend_mem_to_admit();
  }

  /// Must call UpdateMemoryRequirements() at least once before calling this.
  MemLimitSourcePB coord_backend_mem_to_admit_source() const {
    DCHECK(query_schedule_pb_->has_coord_backend_mem_to_admit_source());
    return query_schedule_pb_->coord_backend_mem_to_admit_source();
  }

  void set_largest_min_reservation(const int64_t largest_min_reservation) {
    largest_min_reservation_ = largest_min_reservation;
  }

  void set_coord_min_reservation(const int64_t coord_min_reservation) {
    coord_min_reservation_ = coord_min_reservation;
  }

  /// Returns the Cluster wide memory admitted by the admission controller.
  /// Must call UpdateMemoryRequirements() at least once before calling this.
  int64_t GetClusterMemoryToAdmit() const;

  /// Returns true if coordinator estimates calculated by the planner and specialized for
  /// dedicated a coordinator are to be used for estimating memory requirements.
  /// This happens when the following conditions are true:
  /// 1. Coordinator fragment is scheduled on a dedicated coordinator
  /// 2. Either only the coordinator fragment or no fragments are scheduled on the
  /// coordinator backend. This essentially means that no executor fragments are scheduled
  /// on the coordinator backend.
  bool UseDedicatedCoordEstimates() const;

  /// Populates or updates the per host query memory limit and the amount of memory to be
  /// admitted based on the pool configuration passed to it. Must be called at least once
  /// before making any calls to per_backend_mem_to_admit(), per_backend_mem_limit() and
  /// GetClusterMemoryToAdmit(). If 'clamp_query_mem_limit_backend_mem_limit' is set, the
  /// input 'coord_mem_limit_admission' and 'executor_mem_limit_admission' are used for
  /// capping query memory limit on coordinator and executor backends, respectively.
  void UpdateMemoryRequirements(const TPoolConfig& pool_cfg,
      int64_t coord_mem_limit_admission, int64_t executor_mem_limit_admission);

  const std::string& executor_group() const { return executor_group_; }

  void set_executor_group(string executor_group);

  std::mt19937* rng() { return &rng_; }

 private:
  /// These references are valid for the lifetime of this query schedule because they
  /// are all owned by the enclosing QueryExecState.
  const UniqueIdPB& query_id_;
  const TQueryExecRequest& request_;

  /// The query options from the TClientRequest
  const TQueryOptions& query_options_;

  /// Contains the results of scheduling that will be sent back to the coordinator.
  /// Ownership is transferred to the coordinator after scheduling has completed.
  std::unique_ptr<QuerySchedulePB> query_schedule_pb_;

  /// TODO: move these into QueryState
  RuntimeProfile* summary_profile_;

  /// Maps from plan node id to its fragment idx. Filled in c'tor.
  std::vector<int32_t> plan_node_to_fragment_idx_;

  /// Maps from plan node id to its index in plan.nodes. Filled in c'tor.
  std::vector<int32_t> plan_node_to_plan_node_idx_;

  /// Populated in Init(), then calculated in Scheduler::ComputeFragmentExecParams().
  /// Indexed by fragment idx (TPlanFragment.idx).
  std::vector<FragmentScheduleState> fragment_schedule_states_;

  /// Map from backend address to corresponding BackendScheduleState. Created in
  /// GetOrCreateBackendScheduleState().
  PerBackendScheduleStates per_backend_schedule_states_;

  /// Used to generate consecutive fragment instance ids.
  UniqueIdPB next_instance_id_;

  /// The largest min memory reservation across all executors. Set in
  /// Scheduler::Schedule().
  int64_t largest_min_reservation_ = 0;

  /// The coordinator's backend memory reservation. Set in Scheduler::Schedule().
  int64_t coord_min_reservation_ = 0;

  /// The name of the executor group that this schedule was computed for. Set by the
  /// Scheduler and only valid after scheduling completes successfully.
  std::string executor_group_;

  /// Random number generated used for any randomized decisions during scheduling.
  std::mt19937 rng_;

  /// Map from fragment idx to references into the 'request_'.
  std::unordered_map<int32_t, const TPlanFragment&> fragments_;

  /// Populate fragments_ and fragment_schedule_states_ from request_.plan_exec_info.
  /// Sets is_root_coord_fragment and exchange_input_fragments.
  /// Also populates plan_node_to_fragment_idx_ and plan_node_to_plan_node_idx_.
  void Init();

  /// Returns true if a coordinator fragment is required based on the query stmt type.
  bool RequiresCoordinatorFragment() const {
    return request_.stmt_type == TStmtType::QUERY;
  }

  /// Helper functions to update either
  /// 'query_schedule_pb_->per_backend_mem_to_admit' or
  /// 'query_schedule_pb_->coord_backend_mem_to_admit' along with the limiting reason
  /// 'source' if the 'new_limit' is less or more than the current value.
  void CompareMaxBackendMemToAdmit(
      const int64_t new_limit, const MemLimitSourcePB source);
  void CompareMinBackendMemToAdmit(
      const int64_t new_limit, const MemLimitSourcePB source);
  void CompareMaxCoordinatorMemToAdmit(
      const int64_t new_limit, const MemLimitSourcePB source);
  void CompareMinCoordinatorMemToAdmit(
      const int64_t new_limit, const MemLimitSourcePB source);
};

} // namespace impala
