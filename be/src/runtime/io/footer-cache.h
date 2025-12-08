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
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "common/status.h"
#include "gen-cpp/parquet_types.h"
#include "util/cache/cache.h"
#include "util/container-util.h"
#include "util/metrics-fwd.h"
#include "util/histogram-metric.h"

namespace impala {
namespace io {

/// Type alias for footer cache value - can be empty, Parquet FileMetaData or ORC tail
using FooterCacheValue = std::variant<
    std::monostate,                           // Empty state (cache miss)
    std::shared_ptr<parquet::FileMetaData>,  // Parquet footer
    std::string>;                             // ORC serialized tail

/// The FooterCache is a thread-safe partitioned LRU cache for file footer metadata.
/// It supports both Parquet and ORC file formats by storing different types of footer
/// data. Similar to FileHandleCache, it uses multiple partitions to reduce lock
/// contention in high-concurrency scenarios.
///
/// Cache Key: pair<filename, mtime>
/// Cache Value: variant<shared_ptr<parquet::FileMetaData>, string>
///   - Parquet: shared_ptr<parquet::FileMetaData> (parsed footer object)
///   - ORC: string (serialized tail bytes)
///
/// The cache stores format-specific footer data, which provides:
/// - Parquet: Zero deserialization overhead and complete avoidance of disk I/O
/// - ORC: Avoidance of disk I/O for tail data
/// - Direct use of the cached data by callers
///
/// The cache automatically evicts least recently used entries when capacity is reached.
/// Each partition operates independently with its own lock and LRU list.
class FooterCache {
 public:
  typedef std::pair<std::string, int64_t> CacheKey;

  FooterCache();

  ~FooterCache();

  /// Initialization for the footer cache, including cache and metrics allocation.
  /// Must be called before using the cache.
  Status Init(size_t capacity, size_t num_partitions);

  /// Get a footer from the cache. Returns a variant with std::monostate if not found.
  /// This will hash the filename to determine which partition to use.
  /// The returned variant contains either:
  ///   - std::monostate if not found (cache miss)
  ///   - shared_ptr<parquet::FileMetaData> for Parquet files
  ///   - string for ORC files (serialized tail)
  /// Caller should use std::holds_alternative or std::get to access the value.
  /// Thread-safe.
  FooterCacheValue GetFooter(const std::string& filename, int64_t mtime);

  /// Put a Parquet footer into the cache. Stores the parsed FileMetaData object.
  /// This will hash the filename to determine which partition to use.
  /// Thread-safe.
  Status PutParquetFooter(const std::string& filename, int64_t mtime,
      std::shared_ptr<parquet::FileMetaData> file_metadata);

  /// Put an ORC footer into the cache. Stores the serialized tail bytes.
  /// This will hash the filename to determine which partition to use.
  /// Thread-safe.
  Status PutOrcFooter(const std::string& filename, int64_t mtime,
      const std::string& serialized_tail);

  /// Get cache configuration
  size_t Capacity() const { return total_capacity_; }
  size_t NumPartitions() const { return partitions_.size(); }

  /// Release all resources held by the cache. This will reset all partition caches.
  /// After calling this method, the cache should not be used anymore.
  /// Thread-safe.
  void ReleaseResources();

  /// EvictionCallback for the footer cache.
  class EvictionCallback : public Cache::EvictionCallback {
   public:
    EvictionCallback(IntCounter* entries_evicted, IntGauge* entries_in_use,
        IntGauge* entries_in_use_bytes)
      : footer_cache_entries_evicted_(entries_evicted),
        footer_cache_entries_in_use_(entries_in_use),
        footer_cache_entries_in_use_bytes_(entries_in_use_bytes) {}
    virtual void EvictedEntry(kudu::Slice key, kudu::Slice value) override;

   private:
    /// Metrics for the footer cache.
    IntCounter* footer_cache_entries_evicted_;
    IntGauge* footer_cache_entries_in_use_;
    IntGauge* footer_cache_entries_in_use_bytes_;
  };

 private:
  /// Each partition operates independently with its own Cache instance.
  /// This reduces lock contention in high-concurrency scenarios.
  struct FooterCachePartition {

    std::unique_ptr<Cache> cache;

    Status Init(size_t capacity, int partition_id);
  };

  /// Build a cache key from filename and mtime by concatenating them.
  static std::string BuildMetadataKey(const std::string& filename, int64_t mtime);

  size_t GetPartitionIndex(const std::string& filename) const;

  std::vector<FooterCachePartition> partitions_;

  size_t total_capacity_;

  /// Metrics for the footer cache.
  IntCounter* footer_cache_hits_;
  IntCounter* footer_cache_misses_;
  IntCounter* footer_cache_entries_evicted_;
  IntGauge* footer_cache_entries_in_use_;
  IntGauge* footer_cache_entries_in_use_bytes_;

  /// Statistics for the footer entry sizes.
  HistogramMetric* footer_cache_entry_size_stats_;

  /// Eviction callback function.
  std::unique_ptr<FooterCache::EvictionCallback> evict_callback_;
};

}  // namespace io
}  // namespace impala
