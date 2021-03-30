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

#include "scheduling/query-schedule.h"

#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "runtime/bufferpool/reservation-util.h"
#include "util/mem-info.h"
#include "util/network-util.h"
#include "util/parse-util.h"
#include "util/test-info.h"
#include "util/uid-util.h"

#include "common/names.h"

using boost::uuids::random_generator;
using boost::uuids::uuid;
using namespace impala;

DEFINE_bool_hidden(use_dedicated_coordinator_estimates, true,
    "Hidden option to fall back to legacy memory estimation logic for dedicated"
    " coordinators wherein the same per backend estimate is used for both coordinators "
    "and executors.");

namespace impala {

QuerySchedule::QuerySchedule(const TUniqueId& query_id, const TQueryExecRequest& request,
    const TQueryOptions& query_options, RuntimeProfile* summary_profile,
    RuntimeProfile::EventSequence* query_events)
  : query_id_(query_id),
    request_(request),
    query_options_(query_options),
    summary_profile_(summary_profile),
    query_events_(query_events),
    num_scan_ranges_(0),
    next_instance_id_(query_id) {
  Init();
}

QuerySchedule::QuerySchedule(const TUniqueId& query_id, const TQueryExecRequest& request,
    const TQueryOptions& query_options, RuntimeProfile* summary_profile)
  : query_id_(query_id),
    request_(request),
    query_options_(query_options),
    summary_profile_(summary_profile),
    num_scan_ranges_(0),
    next_instance_id_(query_id) {
  // Init() is not called, this constructor is for white box testing only.
  DCHECK(TestInfo::is_test());
}

void QuerySchedule::Init() {
  // extract TPlanFragments and order by fragment idx
  vector<const TPlanFragment*> fragments;
  for (const TPlanExecInfo& plan_exec_info: request_.plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      fragments.push_back(&fragment);
    }
  }
  sort(fragments.begin(), fragments.end(),
      [](const TPlanFragment* a, const TPlanFragment* b) { return a->idx < b->idx; });

  // this must only be called once
  DCHECK_EQ(fragment_exec_params_.size(), 0);
  for (const TPlanFragment* fragment: fragments) {
    fragment_exec_params_.emplace_back(*fragment);
  }

  // mark coordinator fragment
  const TPlanFragment& root_fragment = request_.plan_exec_info[0].fragments[0];
  if (RequiresCoordinatorFragment()) {
    fragment_exec_params_[root_fragment.idx].is_coord_fragment = true;
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
      FragmentExecParams& dest_params = fragment_exec_params_[dest_idx];
      dest_params.exchange_input_fragments.push_back(fragment.idx);
    }
  }
}

void QuerySchedule::Validate() const {
  // all fragments have a FragmentExecParams
  int num_fragments = 0;
  for (const TPlanExecInfo& plan_exec_info: request_.plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      DCHECK_LT(fragment.idx, fragment_exec_params_.size());
      DCHECK_EQ(fragment.idx, fragment_exec_params_[fragment.idx].fragment.idx);
      ++num_fragments;
    }
  }
  DCHECK_EQ(num_fragments, fragment_exec_params_.size());

  // we assigned the correct number of scan ranges per (host, node id):
  // assemble a map from host -> (map from node id -> #scan ranges)
  unordered_map<TNetworkAddress, map<TPlanNodeId, int>> count_map;
  for (const FragmentExecParams& fp: fragment_exec_params_) {
    for (const FInstanceExecParams& ip: fp.instance_exec_params) {
      auto host_it = count_map.find(ip.host);
      if (host_it == count_map.end()) {
        count_map.insert(make_pair(ip.host, map<TPlanNodeId, int>()));
        host_it = count_map.find(ip.host);
      }
      map<TPlanNodeId, int>& node_map = host_it->second;

      for (const PerNodeScanRanges::value_type& instance_entry: ip.per_node_scan_ranges) {
        TPlanNodeId node_id = instance_entry.first;
        auto count_entry = node_map.find(node_id);
        if (count_entry == node_map.end()) {
          node_map.insert(make_pair(node_id, 0));
          count_entry = node_map.find(node_id);
        }
        count_entry->second += instance_entry.second.size();
      }
    }
  }

  for (const FragmentExecParams& fp: fragment_exec_params_) {
    for (const FragmentScanRangeAssignment::value_type& assignment_entry:
        fp.scan_range_assignment) {
      const TNetworkAddress& host = assignment_entry.first;
      DCHECK_GT(count_map.count(host), 0);
      map<TPlanNodeId, int>& node_map = count_map.find(host)->second;
      for (const PerNodeScanRanges::value_type& node_assignment:
          assignment_entry.second) {
        TPlanNodeId node_id = node_assignment.first;
        DCHECK_GT(node_map.count(node_id), 0)
            << host.hostname << " " << host.port << " node_id=" << node_id;
        DCHECK_EQ(node_map[node_id], node_assignment.second.size());
      }
    }
  }

  // Check that all fragments have instances.
  for (const FragmentExecParams& fp: fragment_exec_params_) {
    DCHECK_GT(fp.instance_exec_params.size(), 0) << fp.fragment;
  }

  for (const auto& elem: per_backend_exec_params_) {
    const BackendExecParams& bp = elem.second;
    DCHECK(!bp.instance_params.empty() || bp.is_coord_backend);
  }
}

int64_t QuerySchedule::GetPerExecutorMemoryEstimate() const {
  DCHECK(request_.__isset.per_host_mem_estimate);
  return request_.per_host_mem_estimate;
}

int64_t QuerySchedule::GetDedicatedCoordMemoryEstimate() const {
  DCHECK(request_.__isset.dedicated_coord_mem_estimate);
  return request_.dedicated_coord_mem_estimate;
}

TUniqueId QuerySchedule::GetNextInstanceId() {
  TUniqueId result = next_instance_id_;
  ++next_instance_id_.lo;
  return result;
}

const TPlanFragment& FInstanceExecParams::fragment() const {
  return fragment_exec_params.fragment;
}

const TPlanFragment* QuerySchedule::GetCoordFragment() const {
  // Only have coordinator fragment for statements that return rows.
  if (request_.stmt_type != TStmtType::QUERY) return nullptr;
  const TPlanFragment* fragment = &request_.plan_exec_info[0].fragments[0];
  DCHECK_EQ(fragment->partition.type, TPartitionType::UNPARTITIONED);
  return fragment;
}

void QuerySchedule::GetTPlanFragments(vector<const TPlanFragment*>* fragments) const {
  fragments->clear();
  for (const TPlanExecInfo& plan_info: request_.plan_exec_info) {
    for (const TPlanFragment& fragment: plan_info.fragments) {
      fragments->push_back(&fragment);
    }
  }
}

const FInstanceExecParams& QuerySchedule::GetCoordInstanceExecParams() const {
  DCHECK(RequiresCoordinatorFragment());
  const TPlanFragment& coord_fragment =  request_.plan_exec_info[0].fragments[0];
  const FragmentExecParams& fragment_params = fragment_exec_params_[coord_fragment.idx];
  DCHECK_EQ(fragment_params.instance_exec_params.size(), 1);
  return fragment_params.instance_exec_params[0];
}

vector<int> FragmentExecParams::GetInstanceIdxs() const {
  vector<int> result;
  for (const FInstanceExecParams& instance_params: instance_exec_params) {
    result.push_back(GetInstanceIdx(instance_params.instance_id));
  }
  return result;
}

int FragmentExecParams::GetNumBackends() const {
  int result = 0;
  const TNetworkAddress* prev_krpc_host = nullptr;
  for (const FInstanceExecParams& instance_params: instance_exec_params) {
    if (prev_krpc_host == nullptr || *prev_krpc_host != instance_params.krpc_host) {
      ++result;
      prev_krpc_host = &instance_params.krpc_host;
    }
  }
  return result;
}

int QuerySchedule::GetNumFragmentInstances() const {
  int total = 0;
  for (const FragmentExecParams& p: fragment_exec_params_) {
    total += p.instance_exec_params.size();
  }
  return total;
}

int64_t QuerySchedule::GetClusterMemoryToAdmit() const {
  // There will always be an entry for the coordinator in per_backend_exec_params_.
  return per_backend_mem_to_admit() * (per_backend_exec_params_.size() - 1)
      + coord_backend_mem_to_admit();
}

bool QuerySchedule::UseDedicatedCoordEstimates() const {
  for (auto& itr : per_backend_exec_params_) {
    if (!itr.second.is_coord_backend) continue;
    auto& coord = itr.second;
    bool is_dedicated_coord = !coord.be_desc.is_executor();
    bool only_coord_fragment_scheduled =
        RequiresCoordinatorFragment() && coord.instance_params.size() == 1;
    bool no_fragment_scheduled = coord.instance_params.size() == 0;
    return FLAGS_use_dedicated_coordinator_estimates && is_dedicated_coord
        && (only_coord_fragment_scheduled || no_fragment_scheduled);
  }
  DCHECK(false)
      << "Coordinator backend should always have a entry in per_backend_exec_params_";
  return false;
}

void QuerySchedule::UpdateMemoryRequirements(const TPoolConfig& pool_cfg) {
  // If the min_query_mem_limit and max_query_mem_limit are not set in the pool config
  // then it falls back to traditional(old) behavior, which means that, it sets the
  // mem_limit if it is set in the query options, else sets it to -1 (no limit).
  bool mimic_old_behaviour =
      pool_cfg.min_query_mem_limit == 0 && pool_cfg.max_query_mem_limit == 0;
  bool use_dedicated_coord_estimates = UseDedicatedCoordEstimates();

  per_backend_mem_to_admit_ = 0;
  coord_backend_mem_to_admit_ = 0;
  bool is_mem_limit_set = false;
  if (query_options().__isset.mem_limit && query_options().mem_limit > 0) {
    per_backend_mem_to_admit_ = query_options().mem_limit;
    coord_backend_mem_to_admit_ = query_options().mem_limit;
    is_mem_limit_set = true;
  }

  if (!is_mem_limit_set) {
    per_backend_mem_to_admit_ = GetPerExecutorMemoryEstimate();
    coord_backend_mem_to_admit_ = use_dedicated_coord_estimates ?
        GetDedicatedCoordMemoryEstimate() :
        GetPerExecutorMemoryEstimate();
    if (!mimic_old_behaviour) {
      int64_t min_mem_limit_required =
          ReservationUtil::GetMinMemLimitFromReservation(largest_min_reservation());
      per_backend_mem_to_admit_ = max(per_backend_mem_to_admit_, min_mem_limit_required);
      int64_t min_coord_mem_limit_required =
          ReservationUtil::GetMinMemLimitFromReservation(coord_min_reservation());
      coord_backend_mem_to_admit_ =
          max(coord_backend_mem_to_admit_, min_coord_mem_limit_required);
    }
  }

  if (!is_mem_limit_set || pool_cfg.clamp_mem_limit_query_option) {
    if (pool_cfg.min_query_mem_limit > 0) {
      per_backend_mem_to_admit_ =
          max(per_backend_mem_to_admit_, pool_cfg.min_query_mem_limit);
      if (!use_dedicated_coord_estimates || is_mem_limit_set) {
        // The minimum mem limit option does not apply to dedicated coordinators -
        // this would result in over-reserving of memory. Treat coordinator and
        // executor mem limits the same if the query option was explicitly set.
        coord_backend_mem_to_admit_ =
            max(coord_backend_mem_to_admit_, pool_cfg.min_query_mem_limit);
      }
    }
    if (pool_cfg.max_query_mem_limit > 0) {
      per_backend_mem_to_admit_ =
          min(per_backend_mem_to_admit_, pool_cfg.max_query_mem_limit);
      coord_backend_mem_to_admit_ =
          min(coord_backend_mem_to_admit_, pool_cfg.max_query_mem_limit);
    }
  }

  // Cap the memory estimate at the amount of physical memory available. The user's
  // provided value or the estimate from planning can each be unreasonable.
  per_backend_mem_to_admit_ = min(per_backend_mem_to_admit_, MemInfo::physical_mem());
  coord_backend_mem_to_admit_ = min(coord_backend_mem_to_admit_, MemInfo::physical_mem());

  // If the query is only scheduled to run on the coordinator.
  if (per_backend_exec_params_.size() == 1 && RequiresCoordinatorFragment()) {
    per_backend_mem_to_admit_ = 0;
  }

  if (mimic_old_behaviour && !is_mem_limit_set) {
    per_backend_mem_limit_ = -1;
    coord_backend_mem_limit_ = -1;
  } else {
    per_backend_mem_limit_ = per_backend_mem_to_admit_;
    coord_backend_mem_limit_ = coord_backend_mem_to_admit_;
  }

  // Finally, enforce the MEM_LIMIT_EXECUTORS query option if MEM_LIMIT is not specified.
  if (!is_mem_limit_set && query_options().__isset.mem_limit_executors
      && query_options().mem_limit_executors > 0) {
    per_backend_mem_to_admit_ = query_options().mem_limit_executors;
    per_backend_mem_limit_ = per_backend_mem_to_admit_;
  }
}

void QuerySchedule::set_executor_group(string executor_group) {
  DCHECK(executor_group_.empty());
  executor_group_ = std::move(executor_group);
}

}
