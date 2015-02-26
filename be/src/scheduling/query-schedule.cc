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

#include "scheduling/query-schedule.h"

#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "util/container-util.h"
#include "util/mem-info.h"
#include "util/network-util.h"
#include "util/uid-util.h"
#include "util/debug-util.h"
#include "util/parse-util.h"
#include "util/llama-util.h"

#include "common/names.h"

using boost::uuids::random_generator;
using boost::uuids::uuid;
using namespace impala;

DEFINE_bool(rm_always_use_defaults, false, "If true, all queries use the same initial"
    " resource requests regardless of their computed resource estimates. Only meaningful "
    "if --enable_rm is set.");
DEFINE_string(rm_default_memory, "4G", "The initial amount of memory that"
    " a query should reserve on each node if either it does not have an available "
    "estimate, or if --rm_always_use_defaults is set.");
DEFINE_int32(rm_default_cpu_vcores, 2, "The initial number of virtual cores that"
    " a query should reserve on each node if either it does not have an available "
    "estimate, or if --rm_always_use_defaults is set.");


namespace impala {

// Default value for the request_timeout in a reservation request. The timeout is the
// max time in milliseconds to wait for a resource request to be fulfilled by Llama.
// The default value of five minutes was determined to be reasonable based on
// experiments on a 20-node cluster with TPCDS 15TB and 8 concurrent clients.
// Over 30% of queries timed out with a reservation timeout of 1 minute but only less
// than 5% timed out when using 5 minutes. Still, the default value is somewhat
// arbitrary and a good value is workload dependent.
const int64_t DEFAULT_REQUEST_TIMEOUT_MS = 5 * 60 * 1000;

QuerySchedule::QuerySchedule(const TUniqueId& query_id,
    const TQueryExecRequest& request, const TQueryOptions& query_options,
    const string& effective_user, RuntimeProfile* summary_profile,
    RuntimeProfile::EventSequence* query_events)
  : query_id_(query_id),
    request_(request),
    query_options_(query_options),
    effective_user_(effective_user),
    summary_profile_(summary_profile),
    query_events_(query_events),
    num_fragment_instances_(0),
    num_hosts_(0),
    num_scan_ranges_(0),
    is_admitted_(false) {
  fragment_exec_params_.resize(request.fragments.size());
  // map from plan node id to fragment index in exec_request.fragments
  vector<PlanNodeId> per_node_fragment_idx;
  for (int i = 0; i < request.fragments.size(); ++i) {
    BOOST_FOREACH(const TPlanNode& node, request.fragments[i].plan.nodes) {
      if (plan_node_to_fragment_idx_.size() < node.node_id + 1) {
        plan_node_to_fragment_idx_.resize(node.node_id + 1);
      }
      plan_node_to_fragment_idx_[node.node_id] = i;
    }
  }
}

int64_t QuerySchedule::GetClusterMemoryEstimate() const {
  DCHECK_GT(num_hosts_, 0);
  const int64_t total_cluster_mem = GetPerHostMemoryEstimate() * num_hosts_;
  DCHECK_GE(total_cluster_mem, 0); // Assume total cluster memory fits in an int64_t.
  return total_cluster_mem;
}

int64_t QuerySchedule::GetPerHostMemoryEstimate() const {
  // Precedence of different estimate sources is:
  // user-supplied RM query option >
  //   server-side defaults (if rm_always_use_defaults == true) >
  //     query option limit >
  //       estimate >
  //         server-side defaults (if rm_always_use_defaults == false)
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
  if (query_options_.__isset.rm_initial_mem && query_options_.rm_initial_mem > 0) {
    per_host_mem = query_options_.rm_initial_mem;
  } else if (FLAGS_rm_always_use_defaults) {
    bool ignored;
    per_host_mem = ParseUtil::ParseMemSpec(FLAGS_rm_default_memory,
        &ignored, 0);
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
  // TODO: Get this limit from Llama (Yarn sets it).
  return min(per_host_mem, MemInfo::physical_mem());
}

int16_t QuerySchedule::GetPerHostVCores() const {
  // Precedence of different estimate sources is:
  // server-side defaults (if rm_always_use_defaults == true) >
  //   computed estimates
  //     server-side defaults (if rm_always_use_defaults == false)
  int16_t v_cpu_cores = FLAGS_rm_default_cpu_vcores;
  if (!FLAGS_rm_always_use_defaults && query_options_.__isset.v_cpu_cores &&
      query_options_.v_cpu_cores > 0) {
    v_cpu_cores = query_options_.v_cpu_cores;
  }

  return v_cpu_cores;
}

void QuerySchedule::GetResourceHostport(const TNetworkAddress& src,
    TNetworkAddress* dst) {
  DCHECK(dst != NULL);
  DCHECK(resource_resolver_.get() != NULL)
      << "resource_resolver_ is NULL, didn't call SetUniqueHosts()?";
  resource_resolver_->GetResourceHostport(src, dst);
}

void QuerySchedule::SetUniqueHosts(const unordered_set<TNetworkAddress>& unique_hosts) {
  unique_hosts_ = unique_hosts;
  resource_resolver_.reset(new ResourceResolver(unique_hosts_));
}

void QuerySchedule::PrepareReservationRequest(const string& pool, const string& user) {
  reservation_request_.resources.clear();
  reservation_request_.version = TResourceBrokerServiceVersion::V1;
  reservation_request_.queue = pool;
  reservation_request_.gang = true;
  // Convert the user name to a short name (e.g. 'user1@domain' to 'user1') because
  // Llama checks group membership based on the short name of the principal.
  reservation_request_.user = llama::GetShortName(user);

  // Set optional request timeout from query options.
  if (query_options_.__isset.reservation_request_timeout) {
    DCHECK_GT(query_options_.reservation_request_timeout, 0);
    reservation_request_.__set_request_timeout(
        query_options_.reservation_request_timeout);
  }

  // Set the reservation timeout from the query options or use a default.
  int64_t timeout = DEFAULT_REQUEST_TIMEOUT_MS;
  if (query_options_.__isset.reservation_request_timeout) {
    timeout = query_options_.reservation_request_timeout;
  }
  reservation_request_.__set_request_timeout(timeout);

  int32_t memory_mb = GetPerHostMemoryEstimate() / 1024 / 1024;
  int32_t v_cpu_cores = GetPerHostVCores();
  // The memory_mb and v_cpu_cores estimates may legitimately be zero,
  // e.g., for constant selects. Do not reserve any resources in those cases.
  if (memory_mb == 0 && v_cpu_cores == 0) return;

  DCHECK(resource_resolver_.get() != NULL)
      << "resource_resolver_ is NULL, didn't call SetUniqueHosts()?";
  random_generator uuid_generator;
  BOOST_FOREACH(const TNetworkAddress& host, unique_hosts_) {
    reservation_request_.resources.push_back(llama::TResource());
    llama::TResource& resource = reservation_request_.resources.back();
    uuid id = uuid_generator();
    resource.client_resource_id.hi = *reinterpret_cast<uint64_t*>(&id.data[0]);
    resource.client_resource_id.lo = *reinterpret_cast<uint64_t*>(&id.data[8]);
    resource.enforcement = llama::TLocationEnforcement::MUST;

    TNetworkAddress resource_hostport;
    resource_resolver_->GetResourceHostport(host, &resource_hostport);
    stringstream ss;
    ss << resource_hostport;
    resource.askedLocation = ss.str();
    resource.memory_mb = memory_mb;
    resource.v_cpu_cores = v_cpu_cores;
  }
}

Status QuerySchedule::ValidateReservation() {
  if (!HasReservation()) return Status("Query schedule does not have a reservation.");
  vector<TNetworkAddress> hosts_missing_resources;
  ResourceResolver resolver(unique_hosts_);
  BOOST_FOREACH(const FragmentExecParams& params, fragment_exec_params_) {
    BOOST_FOREACH(const TNetworkAddress& host, params.hosts) {
      // Ignore the coordinator host which is not contained in unique_hosts_.
      if (unique_hosts_.find(host) == unique_hosts_.end()) continue;
      TNetworkAddress resource_hostport;
      resolver.GetResourceHostport(host, &resource_hostport);
      if (reservation_.allocated_resources.find(resource_hostport) ==
          reservation_.allocated_resources.end()) {
        hosts_missing_resources.push_back(host);
      }
    }
  }
  if (!hosts_missing_resources.empty()) {
    stringstream ss;
    ss << "Failed to validate reservation " << reservation_.reservation_id << "." << endl
       << "Missing resources for hosts [";
    for (int i = 0; i < hosts_missing_resources.size(); ++i) {
      ss << hosts_missing_resources[i];
      if (i + 1 !=  hosts_missing_resources.size()) ss << ", ";
    }
    ss << "]";
    return Status(ss.str());
  }
  return Status::OK();
}

}
