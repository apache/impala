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

#ifndef STATESTORE_QUERY_RESOURCE_MGR_H
#define STATESTORE_QUERY_RESOURCE_MGR_H

#include "common/atomic.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ResourceBrokerService.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/Frontend_types.h"
#include "util/promise.h"
#include "util/thread.h"

#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/shared_ptr.hpp>
#include <string>

namespace impala {

class ResourceBroker;

/// Utility class to map hosts to the Llama-registered resource-holding hosts
/// (i.e. datanodes).
class ResourceResolver {
 public:
  ResourceResolver(const boost::unordered_set<TNetworkAddress>& unique_hosts);

  /// Translates src into a network address suitable for identifying resources across
  /// interactions with the Llama. The MiniLlama expects resources to be requested on
  /// IP:port addresses of Hadoop DNs, whereas the regular Llama only deals with the
  /// hostnames of Yarn NMs. For MiniLlama setups this translation uses the
  /// impalad_to_dn_ mapping to populate dest. When using the regular Llama, this
  /// translation sets a fixed port of 0 in dest because the Llama strips away the port
  /// of resource locations.
  void GetResourceHostport(const TNetworkAddress& src, TNetworkAddress* dst);

 private:
  /// Impala mini clusters using the Mini Llama require translating the impalad hostports
  /// to Hadoop DN hostports registered with the Llama during resource requests
  /// (and then in reverse for translating granted resources to impalads).
  /// These maps form a bi-directional hostport mapping Hadoop DN <-> impalad.
  boost::unordered_map<TNetworkAddress, TNetworkAddress> impalad_to_dn_;
  boost::unordered_map<TNetworkAddress, TNetworkAddress> dn_to_impalad_;

  /// Called only in pseudo-distributed setups (i.e. testing only) to populate
  /// impalad_to_dn_ and dn_to_impalad_
  void CreateLocalLlamaNodeMapping(
      const boost::unordered_set<TNetworkAddress>& unique_hosts);
};

/// Tracks all the state necessary to create expansion requests for all fragments of a
/// single query on a single node. Code that might need to expand the memory reservation
/// for this query (i.e. MemTracker) can use this class to construct expansion requests
/// that may then be submitted to the ResourceBroker.
//
/// If InitCpuAcquisition() is called, this class will monitor the thread token to VCore
/// ratio (thread consumers must use NotifyThreadUsageChange() to update the thread
/// consumption count). If the ratio gets too high (see AboveVcoreSubscriptionThreshold()
/// for details), we will try to acquire more VCore resources from Llama asynchronously.
/// If the ratio passes a higher threshold (see IsVcoreOverSubscribed()), we say that the
/// query fragments are currently oversubscribing their VCore resources.
//
/// Threads are typically handed to a fragment by the thread resource manager, which deals
/// in tokens. When a fragment wants to use a token to start a thread, it should only do so
/// if the ratio of threads to VCores (which map directly onto cgroup shares) is not too
/// large. If it is too large - i.e. the VCores are oversubscribed - the fragment should
/// wait to spin up a new threads until more VCore resources are acquired as above. To help
/// with this, each fragment may register one or more callbacks with their
/// QueryResourceMgr; when more VCore resources are acquired the callbacks are invoked in
/// round-robin fashion. The callback should try and re-acquire the previously untaken
/// thread token, and then a new thread may be started.
//
/// Only CPU-heavy threads need be managed using this class.
//
/// TODO: Handle reducing the number of VCores when threads finish.
/// TODO: Consider combining more closely with ThreadResourceMgr.
/// TODO: Add counters to RuntimeProfile to track resources.
class QueryResourceMgr {
 public:
  QueryResourceMgr(const TUniqueId& reservation_id,
      const TNetworkAddress& local_resource_location, const TUniqueId& query_id);

  /// Must be called only once. Starts a separate thread to monitor thread consumption,
  /// which asks for more VCores from Llama periodically.
  void InitVcoreAcquisition(int32_t init_vcores);

  /// Should be used to check if another thread token may be acquired by this
  /// query. Fragments may ignore this when acquiring a new CPU token, but the result will
  /// be a larger thread:VCore ratio.
  //
  /// Note that this threshold is larger than the one in
  /// AboveVcoreSubscriptionThreshold(). We want to start acquiring more VCore allocations
  /// before we get so oversubscribed that adding new threads is considered a bad idea.
  inline bool IsVcoreOverSubscribed() {
    boost::lock_guard<boost::mutex> l(threads_running_lock_);
    return threads_running_ > vcores_ * max_vcore_oversubscription_ratio_;
  }

  /// Called when thread consumption goes up or down. If the total consumption goes above a
  /// subscription threshold, the acquisition thread will be woken to ask for more VCores.
  void NotifyThreadUsageChange(int delta);

  /// All callbacks registered here are called in round-robin fashion when more VCores are
  /// acquired. Returns a unique ID that can be used as an argument to
  /// RemoveVcoreAvailableCb().
  typedef boost::function<void ()> VcoreAvailableCb;
  int32_t AddVcoreAvailableCb(const VcoreAvailableCb& callback);

  /// Removes the callback with the given ID.
  void RemoveVcoreAvailableCb(int32_t callback_id);

  /// Request an expansion of requested_bytes. If the expansion can be fulfilled within
  /// the timeout period, the number of bytes allocated is returned in allocated_bytes
  /// (which may be more than requested). Otherwise an error status is returned.
  Status RequestMemExpansion(int64_t requested_bytes, int64_t* allocated_bytes);

  /// Sets the exit flag for the VCore acquisiton thread, but does not block. Also clears
  /// the set of callbacks, so that after Shutdown() has returned, no callback will be
  /// invoked.
  void Shutdown();

  /// Waits for the VCore acquisition thread to stop.
  ~QueryResourceMgr();

  const TUniqueId& reservation_id() const { return reservation_id_; }

 private:
  /// ID of the single reservation corresponding to this query
  TUniqueId reservation_id_;

  /// Query ID of the query this class manages resources for.
  TUniqueId query_id_;

  /// Network address of the local service registered with Llama. Usually corresponds to
  /// <local-address>:0, unless a pseudo-dstributed Llama is being used (see
  /// ResourceResolver::CreateLocalLlamaNodeMapping()).
  TNetworkAddress local_resource_location_;

  /// Used to control shutdown of AcquireCpuResources().
  boost::mutex exit_lock_;
  bool exit_;

  /// Protects callbacks_ and callbacks_it_
  boost::mutex callbacks_lock_;

  /// List of callbacks to notify when a new VCore resource is available.
  typedef boost::unordered_map<int32_t, VcoreAvailableCb> CallbackMap;
  CallbackMap callbacks_;

  /// Round-robin iterator to notify callbacks about new VCores one at a time.
  CallbackMap::iterator callbacks_it_;

  /// Total number of callbacks that were ever registered. Used to give each callback a
  /// unique ID so that they can be removed.
  int32_t callback_count_;

  /// Protects threads_running_, threads_changed_cv_ and vcores_.
  boost::mutex threads_running_lock_;

  /// Waited on by AcquireCpuResources(), and notified by NotifyThreadUsageChange().
  boost::condition_variable threads_changed_cv_;

  /// The number of threads we know to be running on behalf of this query.
  int64_t threads_running_;

  /// The number of VCores acquired for this node for this query.
  int64_t vcores_;

  /// Set to FLAGS_max_vcore_oversubscription_ratio in the constructor. If the ratio of
  /// threads to VCores exceeds this number, no more threads may be executed by this query
  /// until more VCore resources are acquired.
  float max_vcore_oversubscription_ratio_;

  /// Runs AcquireVcoreResources() after InitVcoreAcquisition() is called.
  boost::scoped_ptr<Thread> acquire_vcore_thread_;

  /// Signals to the vcore acquisition thread that it should exit after it exits from any
  /// pending Expand() call. Is a shared_ptr so that it will remain valid even after the
  /// parent QueryResourceMgr has been destroyed.
  /// TODO: Combine with ShouldExit(), and replace with AtomicBool when we have such a
  /// thing.
  boost::shared_ptr<AtomicInt<int16_t> > early_exit_;

  /// Signals to the destructor that the vcore acquisition thread is currently in an
  /// Expand() RPC. If so, the destructor does not need to wait for the acquisition thread
  /// to exit.
  boost::shared_ptr<AtomicInt<int16_t> > thread_in_expand_;

  /// Creates the llama resource for the memory and/or cores specified, associated with
  /// the reservation context.
  llama::TResource CreateResource(int64_t memory_mb, int64_t vcores);

  /// Run as a thread owned by acquire_cpu_thread_. Waits for notification from
  /// NotifyThreadUsageChange(), then checks the subscription level to decide if more
  /// VCores are needed, and starts a new expansion request if so.
  void AcquireVcoreResources(boost::shared_ptr<AtomicInt<int16_t> > thread_in_expand,
      boost::shared_ptr<AtomicInt<int16_t> > early_exit);

  /// True if thread:VCore subscription is too high, meaning more VCores are required.
  /// Must be called holding threads_running_ lock.
  bool AboveVcoreSubscriptionThreshold();

  /// Notifies acquire_cpu_thread_ that it should terminate. Does not block.
  bool ShouldExit();
};

}

#endif
