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

#include <memory>
#include <mutex>

#include "common/status.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "util/cache/cache.h"
#include "util/metrics.h"

namespace impala {

class HistogramMetric;
class TupleReader;


/// The TupleCacheMgr maintains per-daemon settings and metadata for the tuple cache.
/// This it used by the various TupleCacheNodes from queries to lookup the cache
/// entries or write cache entries. The TupleCacheMgr maintains the capacity constraint
/// by evicting entries as needed. Unlike the data cache, the tuple cache maintains
/// individual entries in separate files and the files have the cache key incorporated
/// into the file name.
///
/// There are a couple unique features for the TupleCacheMgr that makes it distinct from
/// other caches:
/// 1. It inserts an entry into the cache immediately, even before it knows the total
///    size of the entry. It updates the size of the cache entry later when the entry
///    is completed. This allows the cache to avoid multiple writers trying to create the
///    same entry.
/// 2. When a cache entry is aborted due to its size, the cache keeps a tombstone record
///    to prevent future writers from trying to write that entry. This reduces the
///    overhead of the tuple cache by avoiding writing entries that won't be useful
///    to the cache.
/// For more information about the exact state transitions, see the diagram of the states
/// in tuple-cache-mgr.cc.
class TupleCacheMgr : public Cache::EvictionCallback {
public:
  TupleCacheMgr(MetricGroup* metrics);
  ~TupleCacheMgr() = default;

  // Initialize the TupleCacheMgr. Must be called before any of the other APIs.
  Status Init() WARN_UNUSED_RESULT;

  /// Enum for metric type.
  enum class MetricType {
    HIT,
    MISS,
    SKIPPED,
    HALTED,
  };

  struct Handle;
  class HandleDeleter {
  public:
    void operator()(Handle*) const;
  };

  // UniqueHandle -- a wrapper around opaque Handle structure to facilitate deletion.
  typedef std::unique_ptr<Handle, HandleDeleter> UniqueHandle;

  // Following methods need to be thread-safe. They primarily rely on Cache locking.

  // UniqueHandle lifecycle:
  // 1. Lookup(acquire_write=true). Abandon caching if null.
  // 2a. If IsAvailableForRead, start reading. END
  // 2b. Else if IsAvailableForWrite, start writing.
  // 3. If successful, CompleteWrite. Else AbortWrite. If repeating is also expected to
  //    result in failure, set tombstone. END

  /// Get a handle for the key. If acquire_write and no entry exists, takes a creation
  /// lock and creates a new entry. Null if cache is unavailable.
  UniqueHandle Lookup(const Slice& key, bool acquire_write = false);

  /// Check if entry is complete and ready to read.
  bool IsAvailableForRead(UniqueHandle&) const;

  /// Check if entry is ready to write. Always false if acquire_write=false.
  bool IsAvailableForWrite(UniqueHandle&) const;

  // Register results are complete and available.
  void CompleteWrite(UniqueHandle handle, size_t size);

  // Abort writing. If tombstone is true, IsAvailableForWrite will be false for future
  // queries.
  void AbortWrite(UniqueHandle handle, bool tombstone);

  /// Get path to read/write.
  const char* GetPath(UniqueHandle&) const;

  /// Max size for a single cache entry.
  int MaxSize() const { return cache_->MaxCharge(); }

  /// For metrics increment.
  void IncrementMetric(MetricType type) {
    switch (type) {
      case MetricType::HIT:
        tuple_cache_hits_->Increment(1);
        break;
      case MetricType::MISS:
        tuple_cache_misses_->Increment(1);
        break;
      case MetricType::SKIPPED:
        tuple_cache_skipped_->Increment(1);
        break;
      case MetricType::HALTED:
        tuple_cache_halted_->Increment(1);
        break;
    }
  }

  /// Callback invoked when evicting an entry from the cache. 'key' is the cache key
  /// of the entry being evicted and 'value' contains the cache entry which is the
  /// meta-data of where the cached data is stored.
  virtual void EvictedEntry(kudu::Slice key, kudu::Slice value) override;

 private:
  // Disallow copy and assign
  TupleCacheMgr(const TupleCacheMgr&) = delete;
  TupleCacheMgr& operator=(const TupleCacheMgr&) = delete;

  friend class TupleCacheMgrTest;

  // Constructor for tests
  enum DebugPos {
    FAIL_ALLOCATE = 1 << 0,
    FAIL_INSERT   = 1 << 1,
  };
  TupleCacheMgr(string cache_config, string eviction_policy_str,
      MetricGroup* metrics, uint8_t debug_pos);

  // Delete any existing files in the cache directory to start fresh
  Status DeleteExistingFiles() const;

  const std::string cache_config_;
  const std::string eviction_policy_str_;

  std::string cache_dir_;
  bool enabled_ = false;
  uint8_t debug_pos_;

  /// Metrics for the tuple cache in the daemon level.
  IntCounter* tuple_cache_hits_;
  IntCounter* tuple_cache_misses_;
  IntCounter* tuple_cache_skipped_;
  IntCounter* tuple_cache_halted_;
  IntCounter* tuple_cache_entries_evicted_;
  IntGauge* tuple_cache_entries_in_use_;
  IntGauge* tuple_cache_entries_in_use_bytes_;
  IntGauge* tuple_cache_tombstones_in_use_;

  /// Statistics for the tuple cache sizes allocated.
  HistogramMetric* tuple_cache_entry_size_stats_;

  /// The instance of the cache.
  mutable std::mutex creation_lock_;
  std::unique_ptr<Cache> cache_;
};

}
