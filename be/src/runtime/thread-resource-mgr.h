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

#ifndef IMPALA_RUNTIME_THREAD_RESOURCE_MGR_H
#define IMPALA_RUNTIME_THREAD_RESOURCE_MGR_H

#include <stdlib.h>

#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>

#include <list>

#include "common/status.h"

namespace impala {

/// Singleton object to manage CPU (aka thread) resources for the process.
/// Conceptually, there is a fixed pool of threads that are shared between
/// query fragments.  If there is only one fragment running, it can use the
/// entire pool, spinning up the maximum number of threads to saturate the
/// hardware.  If there are multiple fragments, the CPU pool must be shared
/// between them.  Currently, the total system pool is split evenly between
/// all consumers.  Each consumer gets ceil(total_system_threads / num_consumers).
//
/// Each fragment must register with the ThreadResourceMgr to request threads
/// (in the form of tokens).  The fragment has required threads (it can't run
/// with fewer threads) and optional threads.  If the fragment is running on its
/// own, it will be able to spin up more optional threads.  When the system
/// is under load, the ThreadResourceMgr will stop giving out tokens for optional
/// threads.
/// Pools should not use this for threads that are almost always idle (e.g.
/// periodic reporting threads).
/// Pools will temporarily go over the quota regularly and this is very
/// much by design.  For example, if a pool is running on its own with
/// 4 required threads and 28 optional and another pool is added to the
/// system, the first pool's quota is then cut by half (16 total) and will
/// over time drop the optional threads.
/// This class is thread safe.
/// TODO: this is an initial simple version to improve the behavior with
/// concurrency.  This will need to be expanded post GA.  These include:
///  - More places where threads are optional (e.g. hash table build side,
///    data stream threads, etc).
///  - Admission control
///  - Integration with other nodes/statestore
///  - Priorities for different pools
/// If both the mgr and pool locks need to be taken, the mgr lock must
/// be taken first.
///
/// TODO: make ResourcePool a stand-alone class
class ThreadResourceMgr {
 public:
  class ResourcePool;

  /// This function will be called whenever the pool has more threads it can run on.
  /// This can happen on ReleaseThreadToken or if the quota for this pool increases.
  /// This is a good place, for example, to wake up anything blocked on available threads.
  /// This callback must not block.
  /// Note that this is not called once for each available thread or even guaranteed that
  /// when it is called, a thread is available (the quota could have changed again in
  /// between).  It is simply that something might have happened (similar to condition
  /// variable semantics).
  typedef boost::function<void (ResourcePool*)> ThreadAvailableCb;

  /// Pool abstraction for a single resource pool.
  /// TODO: this is not quite sufficient going forward.  We need a hierarchy of pools,
  /// one for the entire query, and a sub pool for each component that needs threads,
  /// all of which share a quota.  Currently, the way state is tracked here, it would
  /// be impossible to have two components both want optional threads (e.g. two things
  /// that have 1+ thread usage).
  class ResourcePool {
   public:
    /// Acquire a thread for the pool.  This will always succeed; the
    /// pool will go over the quota.
    /// Pools should use this API to reserve threads they need in order
    /// to make progress.
    void AcquireThreadToken();

    /// Try to acquire a thread for this pool.  If the pool is at
    /// the quota, this will return false and the pool should not run.
    /// Pools should use this API for resources they can use but don't
    /// need (e.g. scanner threads).
    bool TryAcquireThreadToken(bool* is_reserved = NULL);

    /// Set a reserved optional number of threads for this pool.  This can be
    /// used to implement that a component needs n+ number of threads.  The
    /// first 'num' threads are guaranteed to be acquirable (via TryAcquireThreadToken)
    /// but anything beyond can fail.
    /// This can also be done with:
    ///  if (pool->num_optional_threads() < num) AcquireThreadToken();
    ///  else TryAcquireThreadToken();
    /// and similar tracking on the Release side but this is common enough to
    /// abstract it away.
    void ReserveOptionalTokens(int num);

    /// Release a thread for the pool.  This must be called once for
    /// each call to AcquireThreadToken and each successful call to TryAcquireThreadToken
    /// If the thread token is from AcquireThreadToken, required must be true; false
    /// if from TryAcquireThreadToken.
    /// Must not be called from from ThreadAvailableCb.
    void ReleaseThreadToken(bool required);

    /// Register a callback to be notified when a thread is available.
    /// Returns a unique id to be used when removing the callback.
    /// TODO: rethink this.  How we do coordinate when we have multiple places in
    /// the execution that all need threads (e.g. do we use that thread for
    /// the scanner or for the join).
    int AddThreadAvailableCb(ThreadAvailableCb fn);

    /// Unregister the callback corresponding to 'id'.
    void RemoveThreadAvailableCb(int id);

    /// Returns the number of threads that are from AcquireThreadToken.
    int num_required_threads() const { return num_threads_ & 0xFFFFFFFF; }

    /// Returns the number of thread resources returned by successful calls
    /// to TryAcquireThreadToken.
    int num_optional_threads() const { return num_threads_ >> 32; }

    /// Returns the total number of thread resources for this pool
    /// (i.e. num_optional_threads + num_required_threads).
    int64_t num_threads() const {
      return num_required_threads() + num_optional_threads();
    }

    int num_reserved_optional_threads() { return num_reserved_optional_threads_; }

    /// Returns true if the number of optional threads has now exceeded the quota.
    bool optional_exceeded() {
      // Cache this so optional/required are computed based on the same value.
      volatile int64_t num_threads = num_threads_;
      int64_t optional_threads = num_threads >> 32;
      int64_t required_threads = num_threads & 0xFFFFFFFF;
      return optional_threads > num_reserved_optional_threads_ &&
             optional_threads + required_threads > quota();
    }

    /// Returns the number of optional threads that can still be used.
    int num_available_threads() const {
      int value = std::max(quota() - static_cast<int>(num_threads()),
          num_reserved_optional_threads_ - num_optional_threads());
      return std::max(0, value);
    }

    /// Returns the quota for this pool.  Note this changes dynamically
    /// based on system load.
    int quota() const { return std::min(max_quota_, parent_->per_pool_quota_); }

    /// Sets the max thread quota for this pool.
    /// The actual quota is the min of this value and the dynamic value.
    void set_max_quota(int quota) { max_quota_ = quota; }

   private:
    friend class ThreadResourceMgr;

    ResourcePool(ThreadResourceMgr* parent);

    /// Resets internal state.
    void Reset();

    /// Invoke registered callbacks in round-robin manner until the quota is exhausted.
    void InvokeCallbacks();

    ThreadResourceMgr* parent_;

    int max_quota_;
    int num_reserved_optional_threads_;

    /// A single 64 bit value to store both the number of optional and
    /// required threads.  This is combined to allow using compare and
    /// swap operations.  The number of required threads is the lower
    /// 32 bits and the number of optional threads is the upper 32 bits.
    int64_t num_threads_;

    /// Lock for the fields below.  This lock is taken when the callback
    /// function is called.
    /// TODO: reconsider this.
    boost::mutex lock_;

    /// A vector of registered callback functions. Entries will be NULL
    /// for unregistered functions.
    std::vector<ThreadAvailableCb> thread_callbacks_;

    /// The number of registered callbacks (i.e. the number of non-NULL entries in
    /// thread_callbacks_).
    int num_callbacks_;

    /// The index into thread_callbacks_ of the next callback to invoke.
    int next_callback_idx_;
  };

  /// Create a thread mgr object.  If threads_quota is non-zero, it will be
  /// the number of threads for the system, otherwise it will be determined
  /// based on the hardware.
  ThreadResourceMgr(int threads_quota = 0);

  int system_threads_quota() const { return system_threads_quota_; }

  /// Register a new pool with the thread mgr.  Registering a pool
  /// will update the quotas for all existing pools.
  ResourcePool* RegisterPool();

  /// Unregisters the pool.  'pool' is no longer valid after this.
  /// This updates the quotas for the remaining pools.
  void UnregisterPool(ResourcePool* pool);

 private:
  /// 'Optimal' number of threads for the entire process.
  int system_threads_quota_;

  /// Lock for the entire object.  Protects all fields below.
  boost::mutex lock_;

  /// Pools currently being managed
  typedef std::set<ResourcePool*> Pools;
  Pools pools_;

  /// Each pool currently gets the same share.  This is the ceil of the
  /// system quota divided by the number of pools.
  int per_pool_quota_;

  /// Recycled list of pool objects
  std::list<ResourcePool*> free_pool_objs_;

  /// Updates the per pool quota and notifies any pools that now have
  /// more threads they can use.  Must be called with lock_ taken.
  /// If new_pool is non-null, new_pool will *not* be notified.
  void UpdatePoolQuotas(ResourcePool* new_pool = NULL);
};

inline void ThreadResourceMgr::ResourcePool::AcquireThreadToken() {
  __sync_fetch_and_add(&num_threads_, 1);
}

inline bool ThreadResourceMgr::ResourcePool::TryAcquireThreadToken(bool* is_reserved) {
  while (true) {
    int64_t previous_num_threads = num_threads_;
    int64_t new_optional_threads = (previous_num_threads >> 32) + 1;
    int64_t new_required_threads = previous_num_threads & 0xFFFFFFFF;
    if (new_optional_threads > num_reserved_optional_threads_ &&
        new_optional_threads + new_required_threads > quota()) {
      return false;
    }
    bool thread_is_reserved = new_optional_threads <= num_reserved_optional_threads_;
    int64_t new_value = new_optional_threads << 32 | new_required_threads;
    // Atomically swap the new value if no one updated num_threads_.  We do not
    // not care about the ABA problem here.
    if (__sync_bool_compare_and_swap(&num_threads_, previous_num_threads, new_value)) {
      if (is_reserved != NULL) *is_reserved = thread_is_reserved;
      return true;
    }
  }
}

inline void ThreadResourceMgr::ResourcePool::ReleaseThreadToken(bool required) {
  if (required) {
    DCHECK_GT(num_required_threads(), 0);
    __sync_fetch_and_add(&num_threads_, -1);
  } else {
    DCHECK_GT(num_optional_threads(), 0);
    while (true) {
      int64_t previous_num_threads = num_threads_;
      int64_t new_optional_threads = (previous_num_threads >> 32) - 1;
      int64_t new_required_threads = previous_num_threads & 0xFFFFFFFF;
      int64_t new_value = new_optional_threads << 32 | new_required_threads;
      if (__sync_bool_compare_and_swap(&num_threads_, previous_num_threads, new_value)) {
        break;
      }
    }
  }
  InvokeCallbacks();
}

} // namespace impala

#endif
