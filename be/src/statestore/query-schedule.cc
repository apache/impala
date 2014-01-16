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

#include "statestore/query-schedule.h"

#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "util/container-util.h"
#include "util/network-util.h"
#include "util/uid-util.h"
#include "util/debug-util.h"

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace boost::uuids;
using namespace impala;

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
    bool is_mini_llama)
  : query_id_(query_id),
    request_(request),
    query_options_(query_options),
    is_mini_llama_(is_mini_llama),
    num_backends_(0),
    num_scan_ranges_(0) {
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

void QuerySchedule::GetResourceHostport(const TNetworkAddress& src,
    TNetworkAddress* dest) {
  if (is_mini_llama_) {
    *dest = impalad_to_dn_[src];
  } else {
    dest->hostname = src.hostname;
    dest->port = 0;
  }
}

void QuerySchedule::CreateMiniLlamaMapping(const vector<string>& llama_nodes) {
  DCHECK(is_mini_llama_);
  DCHECK(!llama_nodes.empty());
  int llama_node_ix = 0;
  BOOST_FOREACH(const TNetworkAddress& host, unique_hosts_) {
    TNetworkAddress dn_hostport = MakeNetworkAddress(llama_nodes[llama_node_ix]);
    impalad_to_dn_[host] = dn_hostport;
    dn_to_impalad_[dn_hostport] = host;
    // Round robin the registered Llama nodes.
    ++llama_node_ix;
    llama_node_ix = llama_node_ix % llama_nodes.size();
  }
}

void QuerySchedule::CreateReservationRequest(const string& pool, const string& user,
    const vector<string>& llama_nodes) {
  yarn_pool_ = pool;
  DCHECK(reservation_request_.get() == NULL);
  reservation_request_.reset(new TResourceBrokerReservationRequest());
  reservation_request_->resources.clear();
  reservation_request_->version = TResourceBrokerServiceVersion::V1;
  reservation_request_->queue = pool;
  reservation_request_->gang = true;
  reservation_request_->user = user;

  // Set optional request timeout from query options.
  if (query_options_.__isset.reservation_request_timeout) {
    DCHECK_GT(query_options_.reservation_request_timeout, 0);
    reservation_request_->__set_request_timeout(
        query_options_.reservation_request_timeout);
  }

  // Set the per-host requested memory and virtual CPU cores.
  // Prefer the manual overrides from the query options over the
  // estimation given in the request.
  // TODO: Remove default values. Not having an estimate or a query option
  // should be an error.
  int32_t memory_mb = 4096;
  if (query_options_.__isset.mem_limit && query_options_.mem_limit > 0) {
    memory_mb = max(1L, query_options_.mem_limit / (1024 * 1024));
  } else if (request_.__isset.per_host_mem_req) {
    memory_mb = request_.per_host_mem_req / (1024 * 1024);
  }
  int16_t v_cpu_cores = 2;
  if (query_options_.__isset.v_cpu_cores && query_options_.v_cpu_cores > 0) {
    v_cpu_cores = query_options_.v_cpu_cores;
  } else if (request_.__isset.per_host_vcores) {
    v_cpu_cores = request_.per_host_vcores;
  }

  // Set the reservation timeout from the query options or use a default.
  int64_t timeout = DEFAULT_REQUEST_TIMEOUT_MS;
  if (query_options_.__isset.reservation_request_timeout) {
    timeout = query_options_.reservation_request_timeout;
  }
  reservation_request_->__set_request_timeout(timeout);

  // The memory_mb and v_cpu_cores estimates may legitimately be zero,
  // e.g., for constant selects. Do not reserve any resources in those cases.
  if (memory_mb == 0 && v_cpu_cores == 0) return;

  if (is_mini_llama_) CreateMiniLlamaMapping(llama_nodes);

  random_generator uuid_generator;
  BOOST_FOREACH(const TNetworkAddress& host, unique_hosts_) {
    reservation_request_->resources.push_back(llama::TResource());
    llama::TResource& resource = reservation_request_->resources.back();
    uuid id = uuid_generator();
    resource.client_resource_id.hi = *reinterpret_cast<uint64_t*>(&id.data[0]);
    resource.client_resource_id.lo = *reinterpret_cast<uint64_t*>(&id.data[8]);
    resource.enforcement = llama::TLocationEnforcement::MUST;

    stringstream ss;
    TNetworkAddress resource_hostport;
    GetResourceHostport(host, &resource_hostport);
    ss << resource_hostport;
    resource.askedLocation = ss.str();
    resource.memory_mb = memory_mb;
    resource.v_cpu_cores = v_cpu_cores;
  }
}

Status QuerySchedule::ValidateReservation() {
  if (!HasReservation()) return Status("Query schedule does not have a reservation.");
  vector<TNetworkAddress> hosts_missing_resources;
  BOOST_FOREACH(const FragmentExecParams& params, fragment_exec_params_) {
    BOOST_FOREACH(const TNetworkAddress& host, params.hosts) {
      // Ignore the coordinator host which is not contained in unique_hosts_.
      if (unique_hosts_.find(host) == unique_hosts_.end()) continue;
      TNetworkAddress resource_hostport;
      GetResourceHostport(host, &resource_hostport);
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
  return Status::OK;
}

}
