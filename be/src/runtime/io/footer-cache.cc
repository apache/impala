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

#include "runtime/io/footer-cache.h"

#include <glog/logging.h>
#include <sstream>

#include "common/names.h"
#include "util/debug-util.h"
#include "util/hash-util.h"
#include "util/metrics.h"
#include "util/impalad-metrics.h"

namespace impala {
namespace io {

// ======================== FooterCache ========================

void FooterCache::EvictionCallback::EvictedEntry(kudu::Slice key, kudu::Slice value) {
  DCHECK(key.data() != nullptr);
  DCHECK(value.data() != nullptr);
  
  // Explicitly destroy the FooterCacheValue (variant) stored in the cache
  FooterCacheValue* footer_value =
      reinterpret_cast<FooterCacheValue*>(const_cast<uint8_t*>(value.data()));
  footer_value->~FooterCacheValue();
  
  int64_t entry_size = key.size() + value.size();
  
  VLOG(2) << "Evicted footer cache entry, key size=" << key.size()
          << ", value size=" << value.size();
  
  // Update statistics, thread-safe
  if (footer_cache_entries_evicted_ != nullptr) {
    footer_cache_entries_evicted_->Increment(1);
  }
  if (footer_cache_entries_in_use_ != nullptr) {
    footer_cache_entries_in_use_->Increment(-1);
  }
  if (footer_cache_entries_in_use_bytes_ != nullptr) {
    footer_cache_entries_in_use_bytes_->Increment(-entry_size);
  }
}

std::string FooterCache::BuildMetadataKey(const std::string& filename, int64_t mtime) {
  // Build cache key by concatenating filename and mtime
  std::ostringstream oss;
  oss << filename << mtime;
  return oss.str();
}

FooterCache::FooterCache()
  : total_capacity_(0),
    footer_cache_hits_(ImpaladMetrics::IO_MGR_FOOTER_CACHE_HITS),
    footer_cache_misses_(ImpaladMetrics::IO_MGR_FOOTER_CACHE_MISSES),
    footer_cache_entries_evicted_(ImpaladMetrics::IO_MGR_FOOTER_CACHE_ENTRIES_EVICTED),
    footer_cache_entries_in_use_(ImpaladMetrics::IO_MGR_FOOTER_CACHE_ENTRIES_IN_USE),
    footer_cache_entries_in_use_bytes_(ImpaladMetrics::IO_MGR_FOOTER_CACHE_ENTRIES_IN_USE_BYTES),
    footer_cache_entry_size_stats_(ImpaladMetrics::IO_MGR_FOOTER_CACHE_ENTRY_SIZES),
    evict_callback_(
        new FooterCache::EvictionCallback(footer_cache_entries_evicted_,
            footer_cache_entries_in_use_, footer_cache_entries_in_use_bytes_)) {}

Status FooterCache::Init(size_t capacity, size_t num_partitions) {
  DCHECK_GT(capacity, 0);
  DCHECK_GT(num_partitions, 0);
  DCHECK(partitions_.empty()) << "FooterCache already initialized";

  total_capacity_ = capacity;
  partitions_.resize(num_partitions);

  // Distribute capacity evenly across partitions, rounding up if necessary
  size_t remainder = capacity % num_partitions;
  size_t base_capacity = capacity / num_partitions;
  size_t partition_capacity = (remainder > 0 ? base_capacity + 1 : base_capacity);

  // Initialize each partition's cache
  for (size_t i = 0; i < num_partitions; ++i) {
    Status status = partitions_[i].Init(partition_capacity, i);
    if (!status.ok()) {
      return Status(Substitute("Failed to initialize footer cache partition $0: $1",
          i, status.GetDetail()));
    }
  }

  VLOG(1) << "Initialized footer cache with capacity " << capacity
          << " and " << num_partitions << " partitions";
  return Status::OK();
}

FooterCache::~FooterCache() {
  ReleaseResources();
}

FooterCacheValue FooterCache::GetFooter(
    const std::string& filename, int64_t mtime) {
  // Hash the filename to get the partition index
  size_t partition_idx = GetPartitionIndex(filename);
  FooterCachePartition& partition = partitions_[partition_idx];

  std::string key_str = BuildMetadataKey(filename, mtime);
  kudu::Slice key(key_str);

  // Lookup in the cache (Cache class handles locking internally)
  Cache::UniqueHandle handle = partition.cache->Lookup(key);
  if (!handle) {
    footer_cache_misses_->Increment(1);
    return FooterCacheValue();  // Return empty variant
  }
  
  footer_cache_hits_->Increment(1);

  kudu::Slice value = partition.cache->Value(handle);
  // The cache stores a FooterCacheValue (variant)
  FooterCacheValue* footer_value =
      reinterpret_cast<FooterCacheValue*>(const_cast<uint8_t*>(value.data()));
  VLOG(2) << "Footer cache hit for file: " << filename << " (mtime=" << mtime << ")";
  return *footer_value;
}

Status FooterCache::PutParquetFooter(const std::string& filename, int64_t mtime,
    std::shared_ptr<parquet::FileMetaData> file_metadata) {
  DCHECK(file_metadata != nullptr);

  size_t partition_idx = GetPartitionIndex(filename);
  FooterCachePartition& partition = partitions_[partition_idx];

  std::string key_str = BuildMetadataKey(filename, mtime);
  kudu::Slice key(key_str);

  // Allocate space for storing the FooterCacheValue (variant)
  size_t value_size = sizeof(FooterCacheValue);
  Cache::UniquePendingHandle pending = partition.cache->Allocate(key, value_size);
  if (!pending) {
    return Status(Substitute("Failed to allocate cache space for footer: $0", filename));
  }

  // Store the variant in the cache using placement new
  uint8_t* cache_value = partition.cache->MutableValue(&pending);
  new (cache_value) FooterCacheValue(file_metadata);

  // Insert into the cache (this handles LRU eviction automatically)
  Cache::UniqueHandle handle = partition.cache->Insert(std::move(pending), evict_callback_.get());
  if (!handle) {
    return Status(Substitute("Failed to insert footer into cache: $0", filename));
  }
  
  // Update metrics, thread-safe
  int64_t entry_size = key.size() + value_size;
  footer_cache_entries_in_use_->Increment(1);
  footer_cache_entries_in_use_bytes_->Increment(entry_size);
  footer_cache_entry_size_stats_->Update(entry_size);

  VLOG(2) << "Cached Parquet footer for file: " << filename << " (mtime=" << mtime << ")";
  return Status::OK();
}

Status FooterCache::PutOrcFooter(const std::string& filename, int64_t mtime,
    const std::string& serialized_tail) {
  DCHECK(!serialized_tail.empty());

  size_t partition_idx = GetPartitionIndex(filename);
  FooterCachePartition& partition = partitions_[partition_idx];

  std::string key_str = BuildMetadataKey(filename, mtime);
  kudu::Slice key(key_str);

  // Allocate space for storing the FooterCacheValue (variant)
  size_t value_size = sizeof(FooterCacheValue);
  Cache::UniquePendingHandle pending = partition.cache->Allocate(key, value_size);
  if (!pending) {
    return Status(Substitute("Failed to allocate cache space for footer: $0", filename));
  }

  // Store the variant in the cache using placement new
  uint8_t* cache_value = partition.cache->MutableValue(&pending);
  new (cache_value) FooterCacheValue(serialized_tail);

  // Insert into the cache (this handles LRU eviction automatically)
  Cache::UniqueHandle handle = partition.cache->Insert(std::move(pending), evict_callback_.get());
  if (!handle) {
    return Status(Substitute("Failed to insert footer into cache: $0", filename));
  }
  
  // Update metrics
  int64_t entry_size = key.size() + value_size;
  footer_cache_entries_in_use_->Increment(1);
  footer_cache_entries_in_use_bytes_->Increment(entry_size);
  footer_cache_entry_size_stats_->Update(entry_size);

  VLOG(2) << "Cached ORC footer for file: " << filename << " (mtime=" << mtime << ")";
  return Status::OK();
}

void FooterCache::ReleaseResources() {
  // Release all cache resources by resetting the cache pointers in each partition
  for (FooterCachePartition& partition : partitions_) {
    if (partition.cache != nullptr) {
      partition.cache.reset();
    }
  }
  VLOG(1) << "Released all footer cache resources";
}

size_t FooterCache::GetPartitionIndex(const std::string& filename) const {
  // Hash the filename and mod by number of partitions
  return HashUtil::Hash(filename.data(), filename.size(), 0) % partitions_.size();
}

// ======================== FooterCachePartition ========================

Status FooterCache::FooterCachePartition::Init(size_t capacity, int partition_id) {
  std::string cache_id = Substitute("footer-cache-partition-$0", partition_id);
  cache.reset(NewCache(Cache::EvictionPolicy::LRU, capacity, cache_id));

  if (cache == nullptr) {
    return Status(Substitute("Failed to create cache for partition $0", partition_id));
  }

  Status status = cache->Init();
  if (!status.ok()) {
    return Status(Substitute("Failed to initialize cache for partition $0: $1",
        partition_id, status.GetDetail()));
  }

  VLOG(1) << "Initialized footer cache partition " << partition_id 
          << " with capacity " << capacity;
  return Status::OK();
}

}  // namespace io
}  // namespace impala
