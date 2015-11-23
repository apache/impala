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

#include "scheduling/query-resource-mgr.h"

#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <gutil/strings/substitute.h>
#include <sstream>

#include "runtime/exec-env.h"
#include "resourcebroker/resource-broker.h"
#include "util/bit-util.h"
#include "util/container-util.h"
#include "util/network-util.h"
#include "util/promise.h"
#include "util/time.h"

#include "common/names.h"

using boost::uuids::random_generator;
using boost::uuids::uuid;
using namespace impala;
using namespace strings;

DEFINE_int64(rm_mem_expansion_timeout_ms, 5000, "The amount of time to wait (ms) "
    "for a memory expansion request.");
DEFINE_double(max_vcore_oversubscription_ratio, 2.5, "(Advanced) The maximum ratio "
    "allowed between running threads and acquired VCore resources for a query's fragments"
    " on a single node");

ResourceResolver::ResourceResolver(const unordered_set<TNetworkAddress>& unique_hosts) {
  if (ExecEnv::GetInstance()->is_pseudo_distributed_llama()) {
    CreateLocalLlamaNodeMapping(unique_hosts);
  }
}

void ResourceResolver::GetResourceHostport(const TNetworkAddress& src,
    TNetworkAddress* dest) {
  if (ExecEnv::GetInstance()->is_pseudo_distributed_llama()) {
    *dest = impalad_to_dn_[src];
  } else {
    dest->hostname = src.hostname;
    dest->port = 0;
  }
}

void ResourceResolver::CreateLocalLlamaNodeMapping(
    const unordered_set<TNetworkAddress>& unique_hosts) {
  DCHECK(ExecEnv::GetInstance()->is_pseudo_distributed_llama());
  const vector<string>& llama_nodes =
      ExecEnv::GetInstance()->resource_broker()->llama_nodes();
  DCHECK(!llama_nodes.empty());
  int llama_node_ix = 0;
  BOOST_FOREACH(const TNetworkAddress& host, unique_hosts) {
    TNetworkAddress dn_hostport = MakeNetworkAddress(llama_nodes[llama_node_ix]);
    impalad_to_dn_[host] = dn_hostport;
    dn_to_impalad_[dn_hostport] = host;
    LOG(INFO) << "Mapping Datanode " << dn_hostport << " to Impalad: " << host;
    // Round robin the registered Llama nodes.
    llama_node_ix = (llama_node_ix + 1) % llama_nodes.size();
  }
}

QueryResourceMgr::QueryResourceMgr(const TUniqueId& reservation_id,
    const TNetworkAddress& local_resource_location, const TUniqueId& query_id)
    : reservation_id_(reservation_id), query_id_(query_id),
      local_resource_location_(local_resource_location), exit_(false), callback_count_(0),
      threads_running_(0), vcores_(0) {
  max_vcore_oversubscription_ratio_ = FLAGS_max_vcore_oversubscription_ratio;
}

void QueryResourceMgr::InitVcoreAcquisition(int32_t init_vcores) {
  LOG(INFO) << "Initialising vcore acquisition thread for query " << PrintId(query_id_)
            << " (" << init_vcores << " initial vcores)";
  DCHECK(acquire_vcore_thread_.get() == NULL)
      << "Double initialisation of QueryResourceMgr::InitCpuAcquisition()";
  vcores_ = init_vcores;

  // These shared pointers to atomic values are used to communicate between the vcore
  // acquisition thread and the class destructor. If the acquisition thread is in the
  // middle of an Expand() call, the destructor might have to wait 5s (the default
  // timeout) to return. This holds up query close operations. So instead check to see if
  // the thread is in Expand(), and if so we set a synchronised flag early_exit_ which it
  // inspects immediately after exiting Expand(), and if true, exits before touching any
  // of the class-wide state (because the destructor may have finished before this point).

  thread_in_expand_.reset(new AtomicInt<int16_t>());
  early_exit_.reset(new AtomicInt<int16_t>());
  acquire_vcore_thread_.reset(
      new Thread("resource-mgmt", Substitute("acquire-cpu-$0", PrintId(query_id_)),
          bind<void>(mem_fn(&QueryResourceMgr::AcquireVcoreResources), this,
              thread_in_expand_, early_exit_)));
}

llama::TResource QueryResourceMgr::CreateResource(int64_t memory_mb, int64_t vcores) {
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
  return res;
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

int32_t QueryResourceMgr::AddVcoreAvailableCb(const VcoreAvailableCb& callback) {
  lock_guard<mutex> l(callbacks_lock_);
  callbacks_[callback_count_] = callback;
  callbacks_it_ = callbacks_.begin();
  return callback_count_++;
}

void QueryResourceMgr::RemoveVcoreAvailableCb(int32_t callback_id) {
  lock_guard<mutex> l(callbacks_lock_);
  CallbackMap::iterator it = callbacks_.find(callback_id);
  DCHECK(it != callbacks_.end()) << "Could not find callback with id: " << callback_id;
  callbacks_.erase(it);
  callbacks_it_ = callbacks_.begin();
}

Status QueryResourceMgr::RequestMemExpansion(int64_t requested_bytes,
    int64_t* allocated_bytes) {
  DCHECK(allocated_bytes != NULL);
  *allocated_bytes = 0;
  int64_t requested_mb = BitUtil::Ceil(requested_bytes, 1024L * 1024L);
  llama::TResource res = CreateResource(max(1L, requested_mb), 0);
  llama::TUniqueId expansion_id;
  llama::TAllocatedResource resource;
  RETURN_IF_ERROR(ExecEnv::GetInstance()->resource_broker()->Expand(reservation_id_,
      res, FLAGS_rm_mem_expansion_timeout_ms, &expansion_id, &resource));

  DCHECK_EQ(resource.v_cpu_cores, 0L) << "Unexpected VCPUs returned by Llama";
  *allocated_bytes = resource.memory_mb * 1024L * 1024L;
  return Status::OK();
}

void QueryResourceMgr::AcquireVcoreResources(
    shared_ptr<AtomicInt<int16_t> > thread_in_expand,
    shared_ptr<AtomicInt<int16_t> > early_exit) {
  // Take a copy because we'd like to print it in some cases after the destructor.
  TUniqueId reservation_id = reservation_id_;
  VLOG_QUERY << "Starting Vcore acquisition for: " << reservation_id;
  while (!ShouldExit()) {
    {
      unique_lock<mutex> l(threads_running_lock_);
      while (!AboveVcoreSubscriptionThreshold() && !ShouldExit()) {
        threads_changed_cv_.wait(l);
      }
    }
    if (ShouldExit()) break;

    llama::TResource res = CreateResource(0L, 1);
    VLOG_QUERY << "Expanding VCore allocation: " << reservation_id_;

    // First signal that we are about to enter a blocking Expand() call.
    thread_in_expand->FetchAndUpdate(1L);

    // TODO: Could cause problems if called during or after a system-wide shutdown
    llama::TAllocatedResource resource;
    llama::TUniqueId expansion_id;
    Status status = ExecEnv::GetInstance()->resource_broker()->Expand(reservation_id,
        res, -1, &expansion_id, &resource);
    thread_in_expand->FetchAndUpdate(-1L);
    // If signalled to exit quickly by the destructor, exit the loop now. It's important
    // to do so without accessing any class variables since they may no longer be valid.
    // Need to check after setting thread_in_expand to avoid a race.
    if (early_exit->FetchAndUpdate(0L) != 0) {
      VLOG_QUERY << "Fragment finished during Expand(): " << reservation_id;
      break;
    }
    if (!status.ok()) {
      VLOG_QUERY << "Could not expand CPU resources for query " << PrintId(query_id_)
                 << ", reservation: " << PrintId(reservation_id_) << ". Error was: "
                 << status.GetDetail();
      // Sleep to avoid flooding the resource broker, particularly if requests are being
      // rejected quickly (and therefore we stay oversubscribed)
      // TODO: configurable timeout
      SleepForMs(250);
      continue;
    }

    DCHECK(resource.v_cpu_cores == 1)
        << "Asked for 1 core, got: " << resource.v_cpu_cores;
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
        callbacks_it_->second();
        if (++callbacks_it_ == callbacks_.end()) callbacks_it_ = callbacks_.begin();
      }
    }
  }
  VLOG_QUERY << "Leaving VCore acquisition thread: " << reservation_id;
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
  // First, set the early exit flag. Then check to see if the thread is in Expand(). If
  // so, the acquisition thread is guaranteed to see early_exit_ == 1L once it finishes
  // Expand(), and will exit immediately. It's therefore safe not to wait for it.
  early_exit_->FetchAndUpdate(1L);
  if (thread_in_expand_->FetchAndUpdate(0L) == 0L) {
    acquire_vcore_thread_->Join();
  }
}
