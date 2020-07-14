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

#include "scheduling/schedule-state.h"

#include "runtime/bufferpool/reservation-util.h"
#include "scheduling/scheduler.h"
#include "util/mem-info.h"
#include "util/test-info.h"
#include "util/uid-util.h"

#include "common/names.h"

DEFINE_bool_hidden(use_dedicated_coordinator_estimates, true,
    "Hidden option to fall back to legacy memory estimation logic for dedicated"
    " coordinators wherein the same per backend estimate is used for both coordinators "
    "and executors.");

namespace impala {

FInstanceScheduleState::FInstanceScheduleState(const UniqueIdPB& instance_id,
    const NetworkAddressPB& host, const NetworkAddressPB& krpc_host,
    int per_fragment_instance_idx, const FragmentScheduleState& fragment_schedule_states)
  : host(host), krpc_host(krpc_host) {
  *exec_params.mutable_instance_id() = instance_id;
  exec_params.set_per_fragment_instance_idx(per_fragment_instance_idx);
  exec_params.set_fragment_idx(fragment_schedule_states.fragment.idx);
}

void FInstanceScheduleState::AddScanRanges(
    int scan_idx, const vector<ScanRangeParamsPB>& scan_ranges) {
  ScanRangesPB& scan_ranges_pb = (*exec_params.mutable_per_node_scan_ranges())[scan_idx];
  *scan_ranges_pb.mutable_scan_ranges() = {scan_ranges.begin(), scan_ranges.end()};
}

FragmentScheduleState::FragmentScheduleState(
    const TPlanFragment& fragment, FragmentExecParamsPB* exec_params)
  : is_coord_fragment(false), fragment(fragment), exec_params(exec_params) {
  exec_params->set_fragment_idx(fragment.idx);
}

ScheduleState::ScheduleState(const UniqueIdPB& query_id, const TQueryExecRequest& request,
    const TQueryOptions& query_options, RuntimeProfile* summary_profile, bool is_test)
  : query_id_(query_id),
    request_(request),
    query_options_(query_options),
    query_schedule_pb_(new QuerySchedulePB()),
    summary_profile_(summary_profile),
    next_instance_id_(query_id) {
  if (is_test) {
    // For tests, don't call Init() and seed the random number generator for deterministic
    // results.
    rng_.seed(rand());
  } else {
    Init();
  }
}

void ScheduleState::Init() {
  *query_schedule_pb_->mutable_query_id() = query_id_;
  // extract TPlanFragments and order by fragment idx
  for (const TPlanExecInfo& plan_exec_info: request_.plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      fragments_.emplace(fragment.idx, fragment);
    }
  }

  // this must only be called once
  DCHECK_EQ(fragment_schedule_states_.size(), 0);
  for (int i = 0; i < fragments_.size(); ++i) {
    auto it = fragments_.find(i);
    DCHECK(it != fragments_.end());
    fragment_schedule_states_.emplace_back(
        it->second, query_schedule_pb_->add_fragment_exec_params());
  }

  // mark coordinator fragment
  const TPlanFragment& root_fragment = request_.plan_exec_info[0].fragments[0];
  if (RequiresCoordinatorFragment()) {
    fragment_schedule_states_[root_fragment.idx].is_coord_fragment = true;
    // the coordinator instance gets index 0, generated instance ids start at 1
    next_instance_id_ = CreateInstanceId(next_instance_id_, 1);
  }

  // find max node id
  int max_node_id = 0;
  for (const TPlanExecInfo& plan_exec_info: request_.plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      for (const TPlanNode& node: fragment.plan.nodes) {
        max_node_id = max(node.node_id, max_node_id);
      }
    }
  }

  // populate plan_node_to_fragment_idx_ and plan_node_to_plan_node_idx_
  plan_node_to_fragment_idx_.resize(max_node_id + 1);
  plan_node_to_plan_node_idx_.resize(max_node_id + 1);
  for (const TPlanExecInfo& plan_exec_info: request_.plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      for (int i = 0; i < fragment.plan.nodes.size(); ++i) {
        const TPlanNode& node = fragment.plan.nodes[i];
        plan_node_to_fragment_idx_[node.node_id] = fragment.idx;
        plan_node_to_plan_node_idx_[node.node_id] = i;
      }
    }
  }

  // compute input fragments
  for (const TPlanExecInfo& plan_exec_info: request_.plan_exec_info) {
    // each fragment sends its output to the fragment containing the destination node
    // of its output sink
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      if (!fragment.output_sink.__isset.stream_sink) continue;
      PlanNodeId dest_node_id = fragment.output_sink.stream_sink.dest_node_id;
      FragmentIdx dest_idx = plan_node_to_fragment_idx_[dest_node_id];
      FragmentScheduleState& dest_state = fragment_schedule_states_[dest_idx];
      dest_state.exchange_input_fragments.push_back(fragment.idx);
    }
  }
}

void ScheduleState::Validate() const {
  // all fragments have a FragmentScheduleState
  int num_fragments = 0;
  for (const TPlanExecInfo& plan_exec_info: request_.plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      DCHECK_LT(fragment.idx, fragment_schedule_states_.size());
      DCHECK_EQ(fragment.idx, fragment_schedule_states_[fragment.idx].fragment.idx);
      ++num_fragments;
    }
  }
  DCHECK_EQ(num_fragments, fragment_schedule_states_.size());

  // we assigned the correct number of scan ranges per (host, node id):
  // assemble a map from host -> (map from node id -> #scan ranges)
  unordered_map<NetworkAddressPB, map<TPlanNodeId, int>> count_map;
  for (const auto& entry : per_backend_schedule_states_) {
    for (const FInstanceExecParamsPB& ip : entry.second.exec_params->instance_params()) {
      auto host_it = count_map.find(entry.second.exec_params->address());
      if (host_it == count_map.end()) {
        count_map.insert(
            make_pair(entry.second.exec_params->address(), map<TPlanNodeId, int>()));
        host_it = count_map.find(entry.second.exec_params->address());
      }
      map<TPlanNodeId, int>& node_map = host_it->second;

      for (const auto& instance_entry : ip.per_node_scan_ranges()) {
        TPlanNodeId node_id = instance_entry.first;
        auto count_entry = node_map.find(node_id);
        if (count_entry == node_map.end()) {
          node_map.insert(make_pair(node_id, 0));
          count_entry = node_map.find(node_id);
        }
        count_entry->second += instance_entry.second.scan_ranges_size();
      }
    }
  }

  for (const FragmentScheduleState& fragment_state : fragment_schedule_states_) {
    for (const FragmentScanRangeAssignment::value_type& assignment_entry :
        fragment_state.scan_range_assignment) {
      const NetworkAddressPB& host = assignment_entry.first;
      DCHECK_GT(count_map.count(host), 0);
      map<TPlanNodeId, int>& node_map = count_map.find(host)->second;
      for (const PerNodeScanRanges::value_type& node_assignment:
          assignment_entry.second) {
        TPlanNodeId node_id = node_assignment.first;
        DCHECK_GT(node_map.count(node_id), 0)
            << host.hostname() << " " << host.port() << " node_id=" << node_id;
        DCHECK_EQ(node_map[node_id], node_assignment.second.size());
      }
    }
  }

  // Check that all fragments have instances.
  for (const FragmentScheduleState& fragment_state : fragment_schedule_states_) {
    DCHECK_GT(fragment_state.instance_states.size(), 0) << fragment_state.fragment;
  }

  // Check that all backends have instances, except possibly the coordaintor backend.
  for (const auto& elem : per_backend_schedule_states_) {
    const BackendExecParamsPB* be_params = elem.second.exec_params;
    DCHECK(!be_params->instance_params().empty() || be_params->is_coord_backend());
  }
}
BackendScheduleState& ScheduleState::GetOrCreateBackendScheduleState(
    const NetworkAddressPB& address) {
  auto it = per_backend_schedule_states_.find(address);
  if (it == per_backend_schedule_states_.end()) {
    BackendExecParamsPB* be_params = query_schedule_pb_->add_backend_exec_params();
    it = per_backend_schedule_states_.emplace(address, BackendScheduleState(be_params))
             .first;
  }
  return it->second;
}

int64_t ScheduleState::GetPerExecutorMemoryEstimate() const {
  DCHECK(request_.__isset.per_host_mem_estimate);
  return request_.per_host_mem_estimate;
}

int64_t ScheduleState::GetDedicatedCoordMemoryEstimate() const {
  DCHECK(request_.__isset.dedicated_coord_mem_estimate);
  return request_.dedicated_coord_mem_estimate;
}

void ScheduleState::IncNumScanRanges(int64_t delta) {
  query_schedule_pb_->set_num_scan_ranges(query_schedule_pb_->num_scan_ranges() + delta);
}

UniqueIdPB ScheduleState::GetNextInstanceId() {
  UniqueIdPB result = next_instance_id_;
  next_instance_id_.set_lo(next_instance_id_.lo() + 1);
  return result;
}

int64_t ScheduleState::GetClusterMemoryToAdmit() const {
  // There will always be an entry for the coordinator in per_backend_schedule_states_.
  return query_schedule_pb_->per_backend_mem_to_admit()
      * (per_backend_schedule_states_.size() - 1)
      + coord_backend_mem_to_admit();
}

bool ScheduleState::UseDedicatedCoordEstimates() const {
  for (const auto& itr : per_backend_schedule_states_) {
    if (!itr.second.exec_params->is_coord_backend()) continue;
    auto& coord = itr.second;
    bool is_dedicated_coord = !coord.be_desc.is_executor();
    bool only_coord_fragment_scheduled =
        RequiresCoordinatorFragment() && coord.exec_params->instance_params().size() == 1;
    bool no_fragment_scheduled = coord.exec_params->instance_params().size() == 0;
    return FLAGS_use_dedicated_coordinator_estimates && is_dedicated_coord
        && (only_coord_fragment_scheduled || no_fragment_scheduled);
  }
  DCHECK(false)
      << "Coordinator backend should always have a entry in per_backend_schedule_states_";
  return false;
}

void ScheduleState::UpdateMemoryRequirements(const TPoolConfig& pool_cfg) {
  // If the min_query_mem_limit and max_query_mem_limit are not set in the pool config
  // then it falls back to traditional(old) behavior, which means that, it sets the
  // mem_limit if it is set in the query options, else sets it to -1 (no limit).
  bool mimic_old_behaviour =
      pool_cfg.min_query_mem_limit == 0 && pool_cfg.max_query_mem_limit == 0;
  bool use_dedicated_coord_estimates = UseDedicatedCoordEstimates();

  int64_t per_backend_mem_to_admit = 0;
  int64_t coord_backend_mem_to_admit = 0;
  bool is_mem_limit_set = false;
  if (query_options().__isset.mem_limit && query_options().mem_limit > 0) {
    per_backend_mem_to_admit = query_options().mem_limit;
    coord_backend_mem_to_admit = query_options().mem_limit;
    is_mem_limit_set = true;
  }

  if (!is_mem_limit_set) {
    per_backend_mem_to_admit = GetPerExecutorMemoryEstimate();
    coord_backend_mem_to_admit = use_dedicated_coord_estimates ?
        GetDedicatedCoordMemoryEstimate() :
        GetPerExecutorMemoryEstimate();
    VLOG(3) << "use_dedicated_coord_estimates=" << use_dedicated_coord_estimates
            << " coord_backend_mem_to_admit=" << coord_backend_mem_to_admit
            << " per_backend_mem_to_admit=" << per_backend_mem_to_admit;
    if (!mimic_old_behaviour) {
      int64_t min_mem_limit_required =
          ReservationUtil::GetMinMemLimitFromReservation(largest_min_reservation());
      per_backend_mem_to_admit = max(per_backend_mem_to_admit, min_mem_limit_required);
      int64_t min_coord_mem_limit_required =
          ReservationUtil::GetMinMemLimitFromReservation(coord_min_reservation());
      coord_backend_mem_to_admit =
          max(coord_backend_mem_to_admit, min_coord_mem_limit_required);
    }
  }

  if (!is_mem_limit_set || pool_cfg.clamp_mem_limit_query_option) {
    if (pool_cfg.min_query_mem_limit > 0) {
      per_backend_mem_to_admit =
          max(per_backend_mem_to_admit, pool_cfg.min_query_mem_limit);
      if (!use_dedicated_coord_estimates || is_mem_limit_set) {
        // The minimum mem limit option does not apply to dedicated coordinators -
        // this would result in over-reserving of memory. Treat coordinator and
        // executor mem limits the same if the query option was explicitly set.
        coord_backend_mem_to_admit =
            max(coord_backend_mem_to_admit, pool_cfg.min_query_mem_limit);
      }
    }
    if (pool_cfg.max_query_mem_limit > 0) {
      per_backend_mem_to_admit =
          min(per_backend_mem_to_admit, pool_cfg.max_query_mem_limit);
      coord_backend_mem_to_admit =
          min(coord_backend_mem_to_admit, pool_cfg.max_query_mem_limit);
    }
  }

  // Cap the memory estimate at the amount of physical memory available. The user's
  // provided value or the estimate from planning can each be unreasonable.
  per_backend_mem_to_admit = min(per_backend_mem_to_admit, MemInfo::physical_mem());
  coord_backend_mem_to_admit = min(coord_backend_mem_to_admit, MemInfo::physical_mem());

  // If the query is only scheduled to run on the coordinator.
  if (per_backend_schedule_states_.size() == 1 && RequiresCoordinatorFragment()) {
    per_backend_mem_to_admit = 0;
  }

  int64_t per_backend_mem_limit;
  if (mimic_old_behaviour && !is_mem_limit_set) {
    per_backend_mem_limit = -1;
    query_schedule_pb_->set_coord_backend_mem_limit(-1);
  } else {
    per_backend_mem_limit = per_backend_mem_to_admit;
    query_schedule_pb_->set_coord_backend_mem_limit(coord_backend_mem_to_admit);
  }

  // Finally, enforce the MEM_LIMIT_EXECUTORS query option if MEM_LIMIT is not specified.
  if (!is_mem_limit_set && query_options().__isset.mem_limit_executors
      && query_options().mem_limit_executors > 0) {
    per_backend_mem_to_admit = query_options().mem_limit_executors;
    per_backend_mem_limit = per_backend_mem_to_admit;
  }

  query_schedule_pb_->set_coord_backend_mem_to_admit(coord_backend_mem_to_admit);
  query_schedule_pb_->set_per_backend_mem_limit(per_backend_mem_limit);
  query_schedule_pb_->set_per_backend_mem_to_admit(per_backend_mem_to_admit);
}

void ScheduleState::set_executor_group(string executor_group) {
  DCHECK(executor_group_.empty());
  executor_group_ = std::move(executor_group);
}

}
