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

#include "runtime/thread-resource-mgr.h"

#include <vector>

#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>

#include "common/logging.h"
#include "util/cpu-info.h"

using namespace boost;
using namespace impala;
using namespace std;

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
  thread_available_fn_ = NULL;
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
  unique_lock<mutex> l(lock_);
  DCHECK(pools_.find(pool) != pools_.end());
  pools_.erase(pool);
  free_pool_objs_.push_back(pool);
  UpdatePoolQuotas();
}

void ThreadResourceMgr::ResourcePool::SetThreadAvailableCb(ThreadAvailableCb fn) {
  unique_lock<mutex> l(lock_);
  DCHECK(thread_available_fn_ == NULL || fn == NULL);
  thread_available_fn_ = fn;
}

void ThreadResourceMgr::UpdatePoolQuotas(ResourcePool* new_pool) {
  if (pools_.empty()) return;
  per_pool_quota_ = 
      ceil(static_cast<double>(system_threads_quota_) / pools_.size());
  for (Pools::iterator it = pools_.begin(); it != pools_.end(); ++it) {
    ResourcePool* pool = *it;
    if (pool == new_pool) continue;
    unique_lock<mutex> l(pool->lock_);
    if (pool->num_available_threads() > 0 && pool->thread_available_fn_ != NULL) {
      pool->thread_available_fn_(pool);
    }
  }
}

