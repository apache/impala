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
    is_admitted_(false) {
  fragment_exec_params_.resize(request.fragments.size());
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

}
