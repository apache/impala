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

#include "statestore/query-resource-mgr.h"

#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <gutil/strings/substitute.h>
#include <sstream>

#include "runtime/exec-env.h"
#include "resourcebroker/resource-broker.h"
#include "util/container-util.h"
#include "util/network-util.h"
#include "util/promise.h"
#include "util/time.h"

using namespace boost;
using namespace boost::uuids;
using namespace impala;
using namespace std;
using namespace strings;

const int64_t DEFAULT_EXPANSION_REQUEST_TIMEOUT_MS = 5000;

DEFINE_double(max_vcore_oversubscription_ratio, 2.5, "(Advanced) The maximum ratio "
    "allowed between running threads and acquired VCore resources for a query's fragments"
    " on a single node");

ResourceResolver::ResourceResolver(const unordered_set<TNetworkAddress>& unique_hosts) {
  ResourceBroker* broker = ExecEnv::GetInstance()->resource_broker();
  is_mini_llama_ = (broker == NULL) ? false : broker->is_mini_llama();
  if (is_mini_llama_) CreateMiniLlamaMapping(unique_hosts);
}

void ResourceResolver::GetResourceHostport(const TNetworkAddress& src,
    TNetworkAddress* dest) {
  if (is_mini_llama_) {
    *dest = impalad_to_dn_[src];
  } else {
    dest->hostname = src.hostname;
    dest->port = 0;
  }
}

void ResourceResolver::CreateMiniLlamaMapping(
    const unordered_set<TNetworkAddress>& unique_hosts) {
  DCHECK(is_mini_llama_);
  const vector<string>& llama_nodes =
      ExecEnv::GetInstance()->resource_broker()->llama_nodes();
  DCHECK(!llama_nodes.empty());
  int llama_node_ix = 0;
  BOOST_FOREACH(const TNetworkAddress& host, unique_hosts) {
    TNetworkAddress dn_hostport = MakeNetworkAddress(llama_nodes[llama_node_ix]);
    impalad_to_dn_[host] = dn_hostport;
    dn_to_impalad_[dn_hostport] = host;
    // Round robin the registered Llama nodes.
    llama_node_ix = (llama_node_ix + 1) % llama_nodes.size();
  }
}

QueryResourceMgr::QueryResourceMgr(const TUniqueId& reservation_id,
    const TNetworkAddress& local_resource_location, const TUniqueId& query_id)
    : reservation_id_(reservation_id), query_id_(query_id),
      local_resource_location_(local_resource_location), exit_(false),
      threads_running_(0), vcores_(0) {
  max_vcore_oversubscription_ratio_ = FLAGS_max_vcore_oversubscription_ratio;
}

void QueryResourceMgr::InitVcoreAcquisition(int32_t init_vcores) {
  LOG(INFO) << "Initialising vcore acquisition thread for query " << PrintId(query_id_)
            << " (" << init_vcores << " initial vcores)";
  DCHECK(acquire_vcore_thread_.get() == NULL)
      << "Double initialisation of QueryResourceMgr::InitCpuAcquisition()";
  vcores_ = init_vcores;
  acquire_vcore_thread_.reset(
      new Thread("resource-mgmt", Substitute("acquire-cpu-$0", PrintId(query_id_)),
      bind<void>(mem_fn(&QueryResourceMgr::AcquireVcoreResources), this)));
}

Status QueryResourceMgr::CreateExpansionRequest(int64_t memory_mb, int64_t vcores,
    TResourceBrokerExpansionRequest* request) {
  DCHECK(request != NULL);
  DCHECK(memory_mb > 0 || vcores > 0);
  DCHECK(reservation_id_ != TUniqueId()) << "Expansion requires existing reservation";

  unordered_set<TNetworkAddress> hosts;
  hosts.insert(local_resource_location_);
  ResourceResolver resolver(hosts);
  llama::TResource res;
  res.memory_mb = memory_mb;
  res.v_cpu_cores = vcores;
  TNetworkAddress res_address;
  resolver.GetResourceHostport(local_resource_location_, &res_address);
  res.__set_askedLocation(TNetworkAddressToString(res_address));

  random_generator uuid_generator;
  uuid id = uuid_generator();
  res.client_resource_id.hi = *reinterpret_cast<uint64_t*>(&id.data[0]);
  res.client_resource_id.lo = *reinterpret_cast<uint64_t*>(&id.data[8]);
  res.enforcement = llama::TLocationEnforcement::MUST;

  request->__set_resource(res);
  request->__set_reservation_id(reservation_id_);
  request->__set_request_timeout(DEFAULT_EXPANSION_REQUEST_TIMEOUT_MS);

  return Status::OK;
}

bool QueryResourceMgr::AboveVcoreSubscriptionThreshold() {
  return threads_running_ > vcores_ * (max_vcore_oversubscription_ratio_ * 0.8);
}

void QueryResourceMgr::NotifyThreadUsageChange(int delta) {
  lock_guard<mutex> l(threads_running_lock_);
  threads_running_ += delta;
  DCHECK(threads_running_ >= 0L);
  if (AboveVcoreSubscriptionThreshold()) threads_changed_cv_.notify_all();
}

void QueryResourceMgr::AddVcoreAvailableCb(const VcoreAvailableCb& callback) {
  lock_guard<mutex> l(callbacks_lock_);
  callbacks_.push_back(callback);
  callbacks_it_ = callbacks_.begin();
}

void QueryResourceMgr::AcquireVcoreResources() {
  while (!ShouldExit()) {
    {
      unique_lock<mutex> l(threads_running_lock_);
      while (!AboveVcoreSubscriptionThreshold() && !ShouldExit()) {
        threads_changed_cv_.wait(l);
      }
    }
    if (ShouldExit()) break;

    TResourceBrokerExpansionRequest request;
    CreateExpansionRequest(0L, 1, &request);
    TResourceBrokerExpansionResponse response;
    LOG(INFO) << "Expanding VCore allocation";
    Status status = ExecEnv::GetInstance()->resource_broker()->Expand(request, &response);
    if (!status.ok()) {
      LOG(INFO) << "Could not expand CPU resources for query " << PrintId(query_id_)
                << ", reservation: " << PrintId(reservation_id_) << ". Error was: "
                << status.GetErrorMsg();
      // Sleep to avoid flooding the resource broker, particularly if requests are being
      // rejected quickly (and therefore we stay oversubscribed)
      // TODO: configurable timeout
      SleepForMs(250);
      continue;
    }

    const llama::TAllocatedResource& resource =
        response.allocated_resources.begin()->second;
    DCHECK(resource.v_cpu_cores == 1);
    vcores_ += resource.v_cpu_cores;

    ExecEnv* exec_env = ExecEnv::GetInstance();
    const string& cgroup =
        exec_env->cgroups_mgr()->UniqueIdToCgroup(PrintId(query_id_, "_"));
    int32_t num_shares = exec_env->cgroups_mgr()->VirtualCoresToCpuShares(vcores_);
    exec_env->cgroups_mgr()->SetCpuShares(cgroup, num_shares);

    // TODO: Only call one callback no matter how many VCores we just added; maybe call
    // all of them?
    {
      lock_guard<mutex> l(callbacks_lock_);
      if (callbacks_.size() != 0) {
        (*callbacks_it_)();
        if (++callbacks_it_ == callbacks_.end()) callbacks_it_ = callbacks_.begin();
      }
    }
  }
  VLOG_QUERY << "Leaving VCore acquisition thread";
}

bool QueryResourceMgr::ShouldExit() {
  lock_guard<mutex> l(exit_lock_);
  return exit_;
}

void QueryResourceMgr::Shutdown() {
  {
    lock_guard<mutex> l(exit_lock_);
    if (exit_) return;
    exit_ = true;
  }
  {
    lock_guard<mutex> l(callbacks_lock_);
    callbacks_.clear();
  }
  threads_changed_cv_.notify_all();
}

QueryResourceMgr::~QueryResourceMgr() {
  if (acquire_vcore_thread_.get() == NULL) return;
  if (!ShouldExit()) Shutdown();
  acquire_vcore_thread_->Join();
}
