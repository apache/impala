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

#include "runtime/thread-resource-mgr.h"

#include <cmath>
#include <mutex>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>

#include "common/logging.h"
#include "util/cpu-info.h"

#include "common/names.h"

using namespace impala;

// Controls the number of threads to run work per core.  It's common to pick 2x
// or 3x the number of cores.  This keeps the cores busy without causing excessive
// thrashing.
DEFINE_int32(num_threads_per_core, 3, "Number of threads per core.");

ThreadResourceMgr::ThreadResourceMgr(int threads_quota) {
  DCHECK_GE(threads_quota, 0);
  if (threads_quota == 0) {
    system_threads_quota_ = CpuInfo::num_cores() * FLAGS_num_threads_per_core;
  } else {
    system_threads_quota_ = threads_quota;
  }
}

ThreadResourcePool::ThreadResourcePool(ThreadResourceMgr* parent)
  : parent_(parent) {
}

unique_ptr<ThreadResourcePool> ThreadResourceMgr::CreatePool() {
  unique_lock<mutex> l(lock_);
  unique_ptr<ThreadResourcePool> pool =
      unique_ptr<ThreadResourcePool>(new ThreadResourcePool(this));
  pools_.insert(pool.get());

  // Added a new pool, update the quotas for each pool.
  UpdatePoolQuotas(pool.get());
  return pool;
}

void ThreadResourceMgr::DestroyPool(unique_ptr<ThreadResourcePool> pool) {
  DCHECK(pool != nullptr);
  DCHECK(pool->parent_ != nullptr) << "Already unregistered";
  DCHECK_EQ(pool->num_callbacks_.Load(), 0);
  unique_lock<mutex> l(lock_);
  DCHECK(pools_.find(pool.get()) != pools_.end());
  pools_.erase(pool.get());
  pool->parent_ = nullptr;
  pool.reset();
  UpdatePoolQuotas();
}

int ThreadResourcePool::AddThreadAvailableCb(ThreadAvailableCb fn) {
  unique_lock<mutex> l(lock_);
  // The id is unique for each callback and is monotonically increasing.
  int id = thread_callbacks_.size();
  thread_callbacks_.push_back(fn);
  num_callbacks_.Add(1);
  return id;
}

void ThreadResourcePool::RemoveThreadAvailableCb(int id) {
  unique_lock<mutex> l(lock_);
  DCHECK(!thread_callbacks_[id].empty());
  DCHECK_GT(num_callbacks_.Load(), 0);
  thread_callbacks_[id].clear();
  num_callbacks_.Add(-1);
}

void ThreadResourcePool::InvokeCallbacks() {
  // We need to grab a lock before issuing the callbacks to prevent the
  // them from being removed while it is happening.
  // Note: this is unlikely to be a big deal for performance currently
  // since this is only called with any frequency on (1) the scanner thread
  // completion path and (2) pool unregistration.
  if (num_available_threads() > 0 && num_callbacks_.Load() > 0) {
    int num_invoked = 0;
    unique_lock<mutex> l(lock_);
    while (num_available_threads() > 0 && num_invoked < num_callbacks_.Load()) {
      DCHECK_LT(next_callback_idx_, thread_callbacks_.size());
      ThreadAvailableCb fn = thread_callbacks_[next_callback_idx_];
      if (LIKELY(!fn.empty())) {
        ++num_invoked;
        fn(this);
      }
      ++next_callback_idx_;
      if (next_callback_idx_ == thread_callbacks_.size()) next_callback_idx_ = 0;
    }
  }
}

void ThreadResourceMgr::UpdatePoolQuotas(ThreadResourcePool* new_pool) {
  if (pools_.empty()) return;
  per_pool_quota_.Store(
      ceil(static_cast<double>(system_threads_quota_) / pools_.size()));
  // Only invoke callbacks on pool unregistration.
  if (new_pool == NULL) {
    for (ThreadResourcePool* pool : pools_) {
      pool->InvokeCallbacks();
    }
  }
}

bool ThreadResourcePool::TryAcquireThreadToken() {
  while (true) {
    int64_t previous_num_threads = num_threads_.Load();
    int64_t new_optional_threads = (previous_num_threads >> OPTIONAL_SHIFT) + 1;
    int64_t new_required_threads = previous_num_threads & REQUIRED_MASK;
    if (new_optional_threads + new_required_threads > quota()) return false;
    int64_t new_value = new_optional_threads << OPTIONAL_SHIFT | new_required_threads;
    // Atomically swap the new value if no one updated num_threads_.  We do not
    // care about the ABA problem here.
    if (num_threads_.CompareAndSwap(previous_num_threads, new_value)) return true;
  }
}

void ThreadResourcePool::ReleaseThreadToken(
    bool required, bool skip_callbacks) {
  if (required) {
    DCHECK_GT(num_required_threads(), 0);
    num_threads_.Add(-1);
  } else {
    DCHECK_GT(num_optional_threads(), 0);
    while (true) {
      int64_t previous_num_threads = num_threads_.Load();
      int64_t new_optional_threads = (previous_num_threads >> OPTIONAL_SHIFT) - 1;
      int64_t new_required_threads = previous_num_threads & REQUIRED_MASK;
      int64_t new_value = new_optional_threads << OPTIONAL_SHIFT | new_required_threads;
      if (num_threads_.CompareAndSwap(previous_num_threads, new_value)) break;
    }
  }
  if (!skip_callbacks) InvokeCallbacks();
}
