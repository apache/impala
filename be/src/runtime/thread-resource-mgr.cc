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

#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/thread/locks.hpp>
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
  per_pool_quota_ = 0;
}

ThreadResourceMgr::ResourcePool::ResourcePool(ThreadResourceMgr* parent)
  : parent_(parent) {
}

void ThreadResourceMgr::ResourcePool::Reset() {
  num_threads_ = 0;
  num_reserved_optional_threads_ = 0;
  thread_callbacks_.clear();
  num_callbacks_ = 0;
  next_callback_idx_ = 0;
  max_quota_ = INT_MAX;
}

void ThreadResourceMgr::ResourcePool::ReserveOptionalTokens(int num) {
  DCHECK_GE(num, 0);
  num_reserved_optional_threads_ = num;
}

ThreadResourceMgr::ResourcePool* ThreadResourceMgr::RegisterPool() {
  unique_lock<mutex> l(lock_);
  ResourcePool* pool = NULL;
  if (free_pool_objs_.empty()) {
    pool = new ResourcePool(this);
  } else {
    pool = free_pool_objs_.front();
    free_pool_objs_.pop_front();
  }

  DCHECK(pool != NULL);
  DCHECK(pools_.find(pool) == pools_.end());
  pools_.insert(pool);
  pool->Reset();

  // Added a new pool, update the quotas for each pool.
  UpdatePoolQuotas(pool);
  return pool;
}

void ThreadResourceMgr::UnregisterPool(ResourcePool* pool) {
  DCHECK(pool != NULL);
  DCHECK_EQ(pool->num_callbacks_, 0);
  unique_lock<mutex> l(lock_);
  DCHECK(pools_.find(pool) != pools_.end());
  pools_.erase(pool);
  free_pool_objs_.push_back(pool);
  UpdatePoolQuotas();
}

int ThreadResourceMgr::ResourcePool::AddThreadAvailableCb(ThreadAvailableCb fn) {
  unique_lock<mutex> l(lock_);
  // The id is unique for each callback and is monotonically increasing.
  int id = thread_callbacks_.size();
  thread_callbacks_.push_back(fn);
  ++num_callbacks_;
  return id;
}

void ThreadResourceMgr::ResourcePool::RemoveThreadAvailableCb(int id) {
  unique_lock<mutex> l(lock_);
  DCHECK(thread_callbacks_[id] != NULL);
  DCHECK_GT(num_callbacks_, 0);
  thread_callbacks_[id] = NULL;
  --num_callbacks_;
}

void ThreadResourceMgr::ResourcePool::InvokeCallbacks() {
  // We need to grab a lock before issuing the callbacks to prevent the
  // them from being removed while it is happening.
  // Note: this is unlikely to be a big deal for performance currently
  // since this is only called with any frequency on (1) the scanner thread
  // completion path and (2) pool unregistration.
  if (num_available_threads() > 0 && num_callbacks_ > 0) {
    int num_invoked = 0;
    unique_lock<mutex> l(lock_);
    while (num_available_threads() > 0 && num_invoked < num_callbacks_) {
      DCHECK_LT(next_callback_idx_, thread_callbacks_.size());
      ThreadAvailableCb fn = thread_callbacks_[next_callback_idx_];
      if (LIKELY(fn != NULL)) {
        ++num_invoked;
        fn(this);
      }
      ++next_callback_idx_;
      if (next_callback_idx_ == thread_callbacks_.size()) next_callback_idx_ = 0;
    }
  }
}

void ThreadResourceMgr::UpdatePoolQuotas(ResourcePool* new_pool) {
  if (pools_.empty()) return;
  per_pool_quota_ =
      ceil(static_cast<double>(system_threads_quota_) / pools_.size());
  // Only invoke callbacks on pool unregistration.
  if (new_pool == NULL) {
    for (Pools::iterator it = pools_.begin(); it != pools_.end(); ++it) {
      ResourcePool* pool = *it;
      pool->InvokeCallbacks();
    }
  }
}
