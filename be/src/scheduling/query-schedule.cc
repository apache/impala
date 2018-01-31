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

namespace impala {

QuerySchedule::QuerySchedule(const TUniqueId& query_id,
    const TQueryExecRequest& request, const TQueryOptions& query_options,
    RuntimeProfile* summary_profile, RuntimeProfile::EventSequence* query_events)
  : query_id_(query_id),
    request_(request),
    query_options_(query_options),
    summary_profile_(summary_profile),
    query_events_(query_events),
    num_scan_ranges_(0),
    next_instance_id_(query_id),
    is_admitted_(false) {
  Init();
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
  if (request_.stmt_type == TStmtType::QUERY) {
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
      dest_params.input_fragments.push_back(fragment.idx);
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
        DCHECK_GT(node_map.count(node_id), 0);
        DCHECK_EQ(node_map[node_id], node_assignment.second.size());
      }
    }
  }
  // TODO: add validation for BackendExecParams
}

int64_t QuerySchedule::GetClusterMemoryEstimate() const {
  DCHECK_GT(per_backend_exec_params_.size(), 0);
  const int64_t total_cluster_mem =
      GetPerHostMemoryEstimate() * per_backend_exec_params_.size();
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

  int64_t per_host_mem = 0L;
  if (has_query_option) {
    per_host_mem = query_option_memory_limit;
  } else {
    DCHECK(request_.__isset.per_host_mem_estimate);
    per_host_mem = request_.per_host_mem_estimate;
  }
  // Cap the memory estimate at the amount of physical memory available. The user's
  // provided value or the estimate from planning can each be unreasonable.
  return min(per_host_mem, MemInfo::physical_mem());
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
  DCHECK_EQ(request_.stmt_type, TStmtType::QUERY);
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

int QuerySchedule::GetNumFragmentInstances() const {
  int total = 0;
  for (const FragmentExecParams& p: fragment_exec_params_) {
    total += p.instance_exec_params.size();
  }
  return total;
}

}
