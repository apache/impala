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

QuerySchedule::QuerySchedule(const TUniqueId& query_id,
    const TQueryExecRequest& request, const TQueryOptions& query_options)
  : query_id_(query_id),
    request_(request),
    query_options_(query_options),
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
  GetHostname(&local_hostname_);
}

void QuerySchedule::GetIpAddress(const TNetworkAddress& src, TNetworkAddress* dest) {
  vector<string> ips;
  Status s = HostnameToIpAddrs(src.hostname, &ips);
  // TODO: This hostname to IP conversion will eventually go away.
  // Ignore the error status for now.
  if (!s.ok()) {
    LOG(ERROR) << "Failed to resolve hostname to IP for " << src;
    return;
  }
  if (!FindFirstNonLocalhost(ips, &dest->hostname)) dest->hostname = "127.0.0.1";
  dest->port = src.port;
}

void QuerySchedule::CreateReservationRequest(const string& pool,
    const vector<string>& llama_nodes,
    TResourceBrokerReservationRequest* reservation_request) {
  reservation_request->resources.clear();
  reservation_request->version = TResourceBrokerServiceVersion::V1;
  reservation_request->queue = pool;
  reservation_request->gang = true;

  // Set optional request timeout from query options.
  if (query_options_.__isset.reservation_request_timeout) {
    DCHECK_GT(query_options_.reservation_request_timeout, 0);
    reservation_request->__set_request_timeout(
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
    memory_mb = request_.per_host_mem_req;
  }
  int16_t v_vpu_cores = 2;
  if (query_options_.__isset.v_cpu_cores) {
    v_vpu_cores = query_options_.v_cpu_cores;
  } else if (request_.__isset.per_host_vcores) {
    v_vpu_cores = request_.per_host_vcores;
  }

  // TODO: Temporary hack to always request at least one resource for testing/debugging.
  if (unique_hosts_.empty()) {
    TNetworkAddress dummy_dn_port = MakeNetworkAddress(llama_nodes[0]);
    unique_hosts_.insert(dummy_dn_port);
  }

  random_generator uuid_generator;
  BOOST_FOREACH(const TNetworkAddress& host, unique_hosts_) {
    reservation_request->resources.push_back(llama::TResource());
    llama::TResource& resource = reservation_request->resources.back();
    uuid id = uuid_generator();
    resource.client_resource_id.hi = *reinterpret_cast<uint64_t*>(&id.data[0]);
    resource.client_resource_id.lo = *reinterpret_cast<uint64_t*>(&id.data[8]);
    resource.enforcement = llama::TLocationEnforcement::MUST;

    stringstream ss;
    TNetworkAddress ip_host;
    GetIpAddress(host, &ip_host);
    ss << ip_host;
    resource.askedLocation = ss.str();
    resource.memory_mb = memory_mb;
    resource.v_cpu_cores = v_vpu_cores;
  }
}

Status QuerySchedule::ValidateReservation() {
  if (!HasReservation()) return Status("Query schedule does not have a reservation.");
  vector<TNetworkAddress> hosts_missing_resources;
  BOOST_FOREACH(const FragmentExecParams& params, fragment_exec_params_) {
    BOOST_FOREACH(const TNetworkAddress& host, params.hosts) {
      TNetworkAddress ip_host;
      GetIpAddress(host, &ip_host);
      if (reservation_.allocated_resources.find(ip_host) ==
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
