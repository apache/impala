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

#pragma once

#include <stdlib.h>

#include <mutex>
#include <boost/function.hpp>

#include <list>

#include "common/atomic.h"
#include "common/status.h"

namespace impala {

/// Singleton object to manage CPU (aka thread) resources for the process.
/// Implements a soft limit on the total number of threads being used across running
/// fragment instances. If there is only one fragment instance running, it can use the
/// entire pool, spinning up the maximum number of threads to saturate the
/// hardware. If there are multiple fragment instances, we try to share evenly
/// between them. Currently, the total system pool is split evenly between
/// all consumers. Each consumer gets ceil(total_system_threads / num_consumers).
//
/// Each fragment instance must register with the ThreadResourceMgr to request threads
/// (in the form of tokens). The fragment instance has required threads (it can't run
/// with fewer threads) and optional threads. If the fragment instance is running on its
/// own, it will be able to spin up more optional threads. When the system is under load,
/// the ThreadResourceMgr will stop giving out tokens for optional threads.
///
/// ThreadResourcePools should not be used for threads that are almost always idle (e.g.
/// periodic reporting threads).
/// ThreadResourcePools will temporarily go over the quota regularly and this is very
/// much by design. For example, if a fragment instance is running on its own with
/// 4 required threads and 28 optional and another fragment instance starts, the first
/// pool's quota is then cut by half (16 total) and will over time drop the optional
/// threads.
///
/// This class is thread safe.
///
/// Note: this is a fairly limited way to manage CPU consumption and has flaws, including:
/// * non-deterministic decisions about resource allocation
/// * lack of integration with admission control
/// * lack of any non-trivial policies such as hierachical limits or priorities.

class ThreadResourcePool;

class ThreadResourceMgr {
 public:
  /// Create a thread mgr object. If threads_quota is non-zero, it will be
  /// the number of threads for the system, otherwise it will be determined
  /// based on the hardware.
  ThreadResourceMgr(int threads_quota = 0);

  int system_threads_quota() const { return system_threads_quota_; }

  /// Create a new pool and register with the thread mgr. Registering a pool
  /// will update the quotas for all existing pools.
  std::unique_ptr<ThreadResourcePool> CreatePool();

  /// Destroy the pool and unregister with the thread mgr. This updates the quotas for
  /// the remaining pools.
  void DestroyPool(std::unique_ptr<ThreadResourcePool> pool);

 private:
  friend class ThreadResourcePool;

  /// 'Optimal' number of threads for the entire process.
  int system_threads_quota_;

  /// Lock for the entire object. Protects all fields below. Must be acquired before
  /// ThreadResourcePool::lock_ if both are held at the same time.
  std::mutex lock_;

  /// Pools currently being managed
  typedef std::set<ThreadResourcePool*> Pools;
  Pools pools_;

  /// Each pool currently gets the same share. This is the ceil of the
  /// system quota divided by the number of pools.
  AtomicInt32 per_pool_quota_{0};

  /// Updates the per pool quota and notifies any pools that now have
  /// more threads they can use. Must be called with lock_ taken.
  /// If new_pool is non-null, new_pool will *not* be notified.
  void UpdatePoolQuotas(ThreadResourcePool* new_pool = nullptr);
};

/// Pool abstraction for a single resource pool.
/// Note; there is no concept of hierarchy - all pools are treated equally even if
/// they belong to the same query..
class ThreadResourcePool {
 public:
  /// This function will be called whenever the pool has more threads it can run on.
  /// This can happen on ReleaseThreadToken or if the quota for this pool increases.
  /// This is a good place, for example, to wake up anything blocked on available threads.
  /// This callback must not block.
  /// Note that this is not called once for each available thread or even guaranteed that
  /// when it is called, a thread is available (the quota could have changed again in
  /// between). It is simply that something might have happened (similar to condition
  /// variable semantics).
  typedef boost::function<void (ThreadResourcePool*)> ThreadAvailableCb;

  ~ThreadResourcePool() { DCHECK(parent_ == nullptr) << "Must unregister pool"; }

  /// Acquire a thread for the pool. This will always succeed; the pool will go over the
  /// quota if needed. Pools should use this API to reserve threads they need in order to
  /// make progress.
  void AcquireThreadToken() {
    int64_t num_threads = num_threads_.Add(1);
    int64_t num_required = num_threads & REQUIRED_MASK;
    DCHECK_LE(num_required, max_required_threads_);
  }

  /// Try to acquire a thread for this pool. If the pool is at the quota, this will
  /// return false and the pool should not run. Pools should use this API for resources
  /// they can use but don't need (e.g. extra scanner threads).
  bool TryAcquireThreadToken();

  /// Release a thread for the pool. This must be called once for each call to
  /// AcquireThreadToken() and each successful call to TryAcquireThreadToken()
  /// If the thread token is from AcquireThreadToken(), required must be true; false
  /// if from TryAcquireThreadToken().
  /// If 'skip_callbacks' is true, ReleaseThreadToken() will not run callbacks to find
  /// a replacement for this thread. This is dangerous and can lead to underutilization
  /// of the system.
  void ReleaseThreadToken(bool required, bool skip_callbacks = false);

  /// Register a callback to be notified when a thread is available.
  /// Returns a unique id to be used when removing the callback.
  /// Note: this is limited because we can't coordinate between multiple places in
  /// execution that could use extra threads (e.g. do we use that thread for a
  /// scanner or for a join).
  int AddThreadAvailableCb(ThreadAvailableCb fn);

  /// Unregister the callback corresponding to 'id'.
  void RemoveThreadAvailableCb(int id);

  /// Returns the number of threads that are from AcquireThreadToken.
  int num_required_threads() const { return num_threads_.Load() & REQUIRED_MASK; }

  /// Returns the number of thread resources returned by successful calls
  /// to TryAcquireThreadToken.
  int num_optional_threads() const { return num_threads_.Load() >> OPTIONAL_SHIFT; }

  /// Returns the total number of thread resources for this pool
  /// (i.e. num_optional_threads + num_required_threads).
  int64_t num_threads() const {
    return num_required_threads() + num_optional_threads();
  }

  /// Returns true if the number of optional threads has now exceeded the quota.
  bool optional_exceeded() {
    // Cache this so optional/required are computed based on the same value.
    int64_t num_threads = num_threads_.Load();
    int64_t optional_threads = num_threads >> OPTIONAL_SHIFT;
    int64_t required_threads = num_threads & REQUIRED_MASK;
    return optional_threads + required_threads > quota();
  }

  /// Returns the number of optional threads that can still be used.
  int num_available_threads() const {
    return std::max(0, quota() - static_cast<int>(num_threads()));
  }

  /// Returns the quota for this pool. Note this changes dynamically based on the global
  /// number of registered resource pools.
  int quota() const { return parent_->per_pool_quota_.Load(); }

  /// Set the maximum number of required threads that will be running at one time.
  /// The caller should not create more required threads than this, otherwise this
  /// will DCHECK. Not thread-safe.
  void set_max_required_threads(int max_required_threads) {
    max_required_threads_ = max_required_threads;
  }

 private:
  friend class ThreadResourceMgr;

  /// Mask to extract required threads from 'num_threads_'.
  static constexpr int64_t REQUIRED_MASK = 0xFFFFFFFF;

  /// Shift to extract optional threads from 'num_threads_'.
  static constexpr int OPTIONAL_SHIFT = 32;

  ThreadResourcePool(ThreadResourceMgr* parent);

  /// Invoke registered callbacks in round-robin manner until the quota is exhausted.
  void InvokeCallbacks();

  /// The parent resource manager. Set to NULL when unregistered.
  ThreadResourceMgr* parent_;

  /// Maximum number of required threads that should be running at one time. DCHECKs
  /// if this is exceeded.
  int64_t max_required_threads_ = std::numeric_limits<int32_t>::max();

  /// A single 64 bit value to store both the number of optional and required threads.
  /// This is combined to allow atomic compare-and-swap of both fields. The number of
  /// required threads is the lower 32 bits and the number of optional threads is the
  /// upper 32 bits.
  AtomicInt64 num_threads_{0};

  /// Lock for the fields below. This lock is taken when the callback function is called.
  /// Must be acquired after ThreadResourceMgr::lock_ if both are held at the same time.
  std::mutex lock_;

  /// A vector of registered callback functions. Entries will be set to "empty" function
  /// objects, which can be constructed with the default ThreadAvailableCb() constructor,
  /// when the function is unregistered.
  std::vector<ThreadAvailableCb> thread_callbacks_;

  /// The number of registered callbacks (i.e. the number of non-NULL entries in
  /// 'thread_callbacks_'). Must hold 'lock_' to write, but can read without holding
  /// 'lock_'.
  AtomicInt32 num_callbacks_{0};

  /// The index of the next callback to invoke in 'thread_callbacks_'. Protected by
  /// 'lock_'.
  int next_callback_idx_ = 0;
};
} // namespace impala
