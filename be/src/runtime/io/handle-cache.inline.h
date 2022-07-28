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

#include <tuple>

#include "runtime/io/handle-cache.h"
#include "runtime/io/hdfs-monitored-ops.h"
#include "util/hash-util.h"
#include "util/impalad-metrics.h"
#include "util/lru-multi-cache.inline.h"
#include "util/metrics.h"
#include "util/time.h"

#ifndef IMPALA_RUNTIME_DISK_IO_MGR_HANDLE_CACHE_INLINE_H
#define IMPALA_RUNTIME_DISK_IO_MGR_HANDLE_CACHE_INLINE_H

namespace impala {
namespace io {

HdfsFileHandle::~HdfsFileHandle() {
  if (hdfs_file_ != nullptr && fs_ != nullptr) {
    VLOG_FILE << "hdfsCloseFile() fid=" << hdfs_file_;
    hdfsCloseFile(fs_, hdfs_file_); // TODO: check return code
  }
  fs_ = nullptr;
  fname_ = nullptr;
  hdfs_file_ = nullptr;
}

Status HdfsFileHandle::Init(HdfsMonitor* monitor) {
  Status status = monitor->OpenHdfsFileWithTimeout(fs_, fname_, O_RDONLY, 0,
      &hdfs_file_);
  // fname_ is no longer needed, null it out
  fname_ = nullptr;
  return status;
}

CachedHdfsFileHandle::CachedHdfsFileHandle(const hdfsFS& fs, const string* fname,
    int64_t mtime)
  : HdfsFileHandle(fs, fname, mtime) {
  ImpaladMetrics::IO_MGR_NUM_CACHED_FILE_HANDLES->Increment(1L);
}

CachedHdfsFileHandle::~CachedHdfsFileHandle() {
  ImpaladMetrics::IO_MGR_NUM_CACHED_FILE_HANDLES->Increment(-1L);
}

FileHandleCache::Accessor::Accessor() : cache_accessor_() {}

FileHandleCache::Accessor::Accessor(
    FileHandleCachePartition::CacheType::Accessor&& cache_accessor)
  : cache_accessor_(std::move(cache_accessor)) {
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(1L);
}

void FileHandleCache::Accessor::Set(
    FileHandleCachePartition::CacheType::Accessor&& cache_accessor) {
  // No change if it was empty before
  if (cache_accessor_.Get()) {
    ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(-1L);
  }

  cache_accessor_ = std::move(cache_accessor);

  // No change if it has received an empty accessor
  if (cache_accessor_.Get()) {
    ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(1L);
  }
}

CachedHdfsFileHandle* FileHandleCache::Accessor::Get() {
  return cache_accessor_.Get();
}

void FileHandleCache::Accessor::Release() {
  if (cache_accessor_.Get()) {
    ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(-1L);
    cache_accessor_.Release();
  }
}

void FileHandleCache::Accessor::Destroy() {
  if (cache_accessor_.Get()) {
    ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(-1L);
    cache_accessor_.Destroy();
  }
}

FileHandleCache::Accessor::~Accessor() {
  if (cache_accessor_.Get()) {
    VLOG_FILE << "hdfsUnbufferFile() fid=" << Get()->file();
    if (hdfsUnbufferFile(Get()->file()) != 0) {
      VLOG_FILE << "FS does not support file handle unbuffering, closing file="
                << cache_accessor_.GetKey()->first;
      Destroy();
    } else {
      // Calling explicit release to handle metrics
      Release();
    }
  }
}

FileHandleCache::FileHandleCache(size_t capacity, size_t num_partitions,
    uint64_t unused_handle_timeout_secs, HdfsMonitor* hdfs_monitor)
  : cache_partitions_(num_partitions),
    unused_handle_timeout_secs_(unused_handle_timeout_secs),
    hdfs_monitor_(hdfs_monitor) {
  DCHECK_GT(num_partitions, 0);

  size_t remainder = capacity % num_partitions;
  size_t base_capacity = capacity / num_partitions;
  size_t partition_capacity = (remainder > 0 ? base_capacity + 1 : base_capacity);

  for (FileHandleCachePartition& p : cache_partitions_) {
    p.cache.SetCapacity(partition_capacity);
  }
}

FileHandleCache::~FileHandleCache() {
  shut_down_promise_.Set(true);
  if (eviction_thread_ != nullptr) eviction_thread_->Join();
}

Status FileHandleCache::Init() {
  return Thread::Create("disk-io-mgr-handle-cache", "File Handle Timeout",
      &FileHandleCache::EvictHandlesLoop, this, &eviction_thread_);
}

Status FileHandleCache::GetFileHandle(const hdfsFS& fs, std::string* fname, int64_t mtime,
    bool require_new_handle, FileHandleCache::Accessor* accessor, bool* cache_hit) {
  DCHECK_GT(mtime, 0);
  // Hash the key and get appropriate partition
  int index = HashUtil::Hash(fname->data(), fname->size(), 0) % cache_partitions_.size();
  FileHandleCachePartition& p = cache_partitions_[index];

  auto cache_key = std::make_pair(*fname, mtime);

  // If this requires a new handle, skip to the creation codepath. Otherwise,
  // find an unused entry with the same mtime
  if (!require_new_handle) {
    auto cache_accessor = p.cache.Get(cache_key);

    if (cache_accessor.Get()) {
      // Found a handler in cache and reserved it
      *cache_hit = true;
      accessor->Set(std::move(cache_accessor));
      return Status::OK();
    }
  }

  // There was no entry that was free or caller asked for a new handle
  *cache_hit = false;

  // Emplace a new file handle and get access
  auto accessor_tmp = p.cache.EmplaceAndGet(cache_key, fs, fname, mtime);

  // Opening a file handle requires talking to the NameNode so it can take some time.
  Status status = accessor_tmp.Get()->Init(hdfs_monitor_);
  if (UNLIKELY(!status.ok())) {
    // Removing the handler from the cache after failed initialization.
    accessor_tmp.Destroy();
    return status;
  }

  // Moving the cache accessor to the in/out parameter
  accessor->Set(std::move(accessor_tmp));

  return Status::OK();
}

void FileHandleCache::EvictHandlesLoop() {
  while (true) {
    if (unused_handle_timeout_secs_) {
      for (FileHandleCachePartition& p : cache_partitions_) {
        uint64_t now = MonotonicSeconds();
        uint64_t oldest_allowed_timestamp =
            now > unused_handle_timeout_secs_ ? now - unused_handle_timeout_secs_ : 0;
        p.cache.EvictOlderThan(oldest_allowed_timestamp);
      }
    }

    // This Get() will time out until shutdown, when the promise is set.
    bool timed_out;
    shut_down_promise_.Get(EVICT_HANDLES_PERIOD_MS, &timed_out);
    if (!timed_out) break;
  }
  // The promise must be set to true.
  DCHECK(shut_down_promise_.IsSet());
  DCHECK(shut_down_promise_.Get());
}

}
}
#endif
