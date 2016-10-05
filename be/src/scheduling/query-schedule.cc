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

#include "util/container-util.h"
#include "util/mem-info.h"
#include "util/network-util.h"
#include "util/uid-util.h"
#include "util/debug-util.h"
#include "util/parse-util.h"

#include "common/names.h"

using boost::uuids::random_generator;
using boost::uuids::uuid;
using namespace impala;

// TODO: Remove for Impala 3.0.
DEFINE_bool(rm_always_use_defaults, false, "Deprecated");
DEFINE_string(rm_default_memory, "4G", "Deprecated");
DEFINE_int32(rm_default_cpu_vcores, 2, "Deprecated");

namespace impala {

QuerySchedule::QuerySchedule(const TUniqueId& query_id,
    const TQueryExecRequest& request, const TQueryOptions& query_options,
    RuntimeProfile* summary_profile, RuntimeProfile::EventSequence* query_events)
  : query_id_(query_id),
    request_(request),
    query_options_(query_options),
    summary_profile_(summary_profile),
    query_events_(query_events),
    num_fragment_instances_(0),
    num_scan_ranges_(0),
    next_instance_id_(query_id),
    is_admitted_(false) {
  fragment_exec_params_.resize(request.fragments.size());
  bool is_mt_execution = request.query_ctx.request.query_options.mt_dop > 0;

  if (is_mt_execution) {
    /// TODO-MT: remove else branch and move MtInit() logic here
    MtInit();
  } else {
    // Build two maps to map node ids to their fragments as well as to the offset in their
    // fragment's plan's nodes list.
    for (int i = 0; i < request.fragments.size(); ++i) {
      int node_idx = 0;
      for (const TPlanNode& node: request.fragments[i].plan.nodes) {
        if (plan_node_to_fragment_idx_.size() < node.node_id + 1) {
          plan_node_to_fragment_idx_.resize(node.node_id + 1);
          plan_node_to_plan_node_idx_.resize(node.node_id + 1);
        }
        DCHECK_EQ(plan_node_to_fragment_idx_.size(), plan_node_to_plan_node_idx_.size());
        plan_node_to_fragment_idx_[node.node_id] = i;
        plan_node_to_plan_node_idx_[node.node_id] = node_idx;
        ++node_idx;
      }
    }
  }
}

void QuerySchedule::MtInit() {
  // extract TPlanFragments and order by fragment id
  vector<const TPlanFragment*> fragments;
  for (const TPlanExecInfo& plan_exec_info: request_.mt_plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      fragments.push_back(&fragment);
    }
  }
  sort(fragments.begin(), fragments.end(),
      [](const TPlanFragment* a, const TPlanFragment* b) { return a->idx < b->idx; });

  DCHECK_EQ(mt_fragment_exec_params_.size(), 0);
  for (const TPlanFragment* fragment: fragments) {
    mt_fragment_exec_params_.emplace_back(*fragment);
  }

  // mark coordinator fragment
  const TPlanFragment& coord_fragment = request_.mt_plan_exec_info[0].fragments[0];
  if (coord_fragment.partition.type == TPartitionType::UNPARTITIONED) {
    mt_fragment_exec_params_[coord_fragment.idx].is_coord_fragment = true;
    next_instance_id_.lo = 1;  // generated instance ids start at 1
  }

  // compute input fragments and find max node id
  int max_node_id = 0;
  for (const TPlanExecInfo& plan_exec_info: request_.mt_plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      for (const TPlanNode& node: fragment.plan.nodes) {
        max_node_id = max(node.node_id, max_node_id);
      }
    }

    // fragments[i] sends its output to fragments[dest_fragment_idx[i-1]]
    for (int i = 1; i < plan_exec_info.fragments.size(); ++i) {
      const TPlanFragment& fragment = plan_exec_info.fragments[i];
      FragmentIdx dest_idx =
          plan_exec_info.fragments[plan_exec_info.dest_fragment_idx[i - 1]].idx;
      MtFragmentExecParams& dest_params = mt_fragment_exec_params_[dest_idx];
      dest_params.input_fragments.push_back(fragment.idx);
    }
  }

  // populate plan_node_to_fragment_idx_ and plan_node_to_plan_node_idx_
  plan_node_to_fragment_idx_.resize(max_node_id + 1);
  plan_node_to_plan_node_idx_.resize(max_node_id + 1);
  for (const TPlanExecInfo& plan_exec_info: request_.mt_plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      for (int i = 0; i < fragment.plan.nodes.size(); ++i) {
        const TPlanNode& node = fragment.plan.nodes[i];
        plan_node_to_fragment_idx_[node.node_id] = fragment.idx;
        plan_node_to_plan_node_idx_[node.node_id] = i;
      }
    }
  }
}


int64_t QuerySchedule::GetClusterMemoryEstimate() const {
  DCHECK_GT(unique_hosts_.size(), 0);
  const int64_t total_cluster_mem = GetPerHostMemoryEstimate() * unique_hosts_.size();
  DCHECK_GE(total_cluster_mem, 0); // Assume total cluster memory fits in an int64_t.
  return total_cluster_mem;
}

int64_t QuerySchedule::GetPerHostMemoryEstimate() const {
  // Precedence of different estimate sources is:
  // user-supplied RM query option >
  //     query option limit >
  //       estimate >
  //         server-side defaults
  int64_t query_option_memory_limit = numeric_limits<int64_t>::max();
  bool has_query_option = false;
  if (query_options_.__isset.mem_limit && query_options_.mem_limit > 0) {
    query_option_memory_limit = query_options_.mem_limit;
    has_query_option = true;
  }

  int64_t estimate_limit = numeric_limits<int64_t>::max();
  bool has_estimate = false;
  if (request_.__isset.per_host_mem_req && request_.per_host_mem_req > 0) {
    estimate_limit = request_.per_host_mem_req;
    has_estimate = true;
  }

  int64_t per_host_mem = 0L;
  // TODO: Remove rm_initial_mem and associated logic when we're sure that clients won't
  // be affected.
  if (query_options_.__isset.rm_initial_mem && query_options_.rm_initial_mem > 0) {
    per_host_mem = query_options_.rm_initial_mem;
  } else if (has_query_option) {
    per_host_mem = query_option_memory_limit;
  } else if (has_estimate) {
    per_host_mem = estimate_limit;
  } else {
    // If no estimate or query option, use the server-side limits anyhow.
    bool ignored;
    per_host_mem = ParseUtil::ParseMemSpec(FLAGS_rm_default_memory,
        &ignored, 0);
  }
  // Cap the memory estimate at the amount of physical memory available. The user's
  // provided value or the estimate from planning can each be unreasonable.
  return min(per_host_mem, MemInfo::physical_mem());
}

void QuerySchedule::SetUniqueHosts(const unordered_set<TNetworkAddress>& unique_hosts) {
  unique_hosts_ = unique_hosts;
}

TUniqueId QuerySchedule::GetNextInstanceId() {
  TUniqueId result = next_instance_id_;
  ++next_instance_id_.lo;
  return result;
}

const TPlanFragment& FInstanceExecParams::fragment() const {
  return fragment_exec_params.fragment;
}

int QuerySchedule::GetNumFragmentInstances() const {
  int result = 0;
  if (mt_fragment_exec_params_.empty()) {
    DCHECK(!fragment_exec_params_.empty());
    for (const FragmentExecParams& fragment_exec_params : fragment_exec_params_) {
      result += fragment_exec_params.hosts.size();
    }
  } else {
    for (const MtFragmentExecParams& fragment_exec_params : mt_fragment_exec_params_) {
      result += fragment_exec_params.instance_exec_params.size();
    }
  }
  return result;
}

const TPlanFragment* QuerySchedule::GetCoordFragment() const {
  // Only have coordinator fragment for statements that return rows.
  if (request_.stmt_type != TStmtType::QUERY) return nullptr;
  bool is_mt_exec = request_.query_ctx.request.query_options.mt_dop > 0;
  const TPlanFragment* fragment = is_mt_exec
      ? &request_.mt_plan_exec_info[0].fragments[0] : &request_.fragments[0];

    return fragment;
}

void QuerySchedule::GetTPlanFragments(vector<const TPlanFragment*>* fragments) const {
  fragments->clear();
  bool is_mt_exec = request_.query_ctx.request.query_options.mt_dop > 0;
  if (is_mt_exec) {
    for (const TPlanExecInfo& plan_info: request_.mt_plan_exec_info) {
      for (const TPlanFragment& fragment: plan_info.fragments) {
        fragments->push_back(&fragment);
      }
    }
  } else {
    for (const TPlanFragment& fragment: request_.fragments) {
      fragments->push_back(&fragment);
    }
  }
}

const FInstanceExecParams& QuerySchedule::GetCoordInstanceExecParams() const {
  const TPlanFragment& coord_fragment =  request_.mt_plan_exec_info[0].fragments[0];
  DCHECK_EQ(coord_fragment.partition.type, TPartitionType::UNPARTITIONED);
  const MtFragmentExecParams* fragment_params =
      &mt_fragment_exec_params_[coord_fragment.idx];
  DCHECK(fragment_params != nullptr);
  DCHECK_EQ(fragment_params->instance_exec_params.size(), 1);
  return fragment_params->instance_exec_params[0];
}

}
