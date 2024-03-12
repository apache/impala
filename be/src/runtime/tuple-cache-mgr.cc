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

#include "runtime/tuple-cache-mgr.h"

#include <boost/filesystem.hpp>

#include "common/logging.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "util/filesystem-util.h"
#include "util/histogram-metric.h"
#include "util/kudu-status-util.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/test-info.h"

#include "common/names.h"

namespace filesystem = boost::filesystem;

using kudu::JoinPathSegments;
using strings::SkipEmpty;
using strings::Split;

// We ensure each impalad process gets a unique directory because currently cache keys
// can be shared across instances but represent different scan ranges.
DEFINE_string(tuple_cache, "", "The configuration string for the tuple cache. "
    "The default is the empty string, which disables the tuple cache. The configuration "
    "string is expected to be a directory followed by a ':' and a capacity quota. "
    "For example, /data/0:1TB means the cache may use 1TB in /data/0. Please note that "
    "each Impala daemon on a host must have a unique cache directory.");
DEFINE_string(tuple_cache_eviction_policy, "LRU",
    "(Advanced) The cache eviction policy to use for the tuple cache. "
    "Either 'LRU' (default) or 'LIRS' (experimental)");
// Global feature flag for tuple caching. If false, enable_tuple_cache cannot be true
// and the coordinator cannot produce plans with TupleCacheNodes. The tuple_cache
// parameter also cannot be specified.
DEFINE_bool(allow_tuple_caching, false, "If false, tuple caching cannot be used.");

namespace impala {

// Minimum tuple cache capacity is 64MB.
static const int64_t MIN_TUPLE_CACHE_CAPACITY_BYTES = 64L << 20;
static const char* MIN_TUPLE_CACHE_CAPACITY_STR = "64MB";

// Maximum tuple cache entry size for the purposes of sizing the histogram of
// entry sizes. This does not currently constrain the actual entries.
static constexpr int64_t STATS_MAX_TUPLE_CACHE_ENTRY_SIZE = 128L << 20;

static const char* CACHE_FILE_PREFIX = "tuple-cache-";

TupleCacheMgr::TupleCacheMgr(MetricGroup* metrics)
  : TupleCacheMgr(FLAGS_tuple_cache, FLAGS_tuple_cache_eviction_policy, metrics, 0) {}

TupleCacheMgr::TupleCacheMgr(string cache_config, string eviction_policy_str,
    MetricGroup* metrics, uint8_t debug_pos)
  : cache_config_(move(cache_config)),
    eviction_policy_str_(move(eviction_policy_str)),
    debug_pos_(debug_pos),
    tuple_cache_hits_(metrics->AddCounter("impala.tuple-cache.hits", 0)),
    tuple_cache_misses_(metrics->AddCounter("impala.tuple-cache.misses", 0)),
    tuple_cache_skipped_(metrics->AddCounter("impala.tuple-cache.skipped", 0)),
    tuple_cache_halted_(metrics->AddCounter("impala.tuple-cache.halted", 0)),
    tuple_cache_entries_evicted_(
        metrics->AddCounter("impala.tuple-cache.entries-evicted", 0)),
    tuple_cache_entries_in_use_(
        metrics->AddGauge("impala.tuple-cache.entries-in-use", 0)),
    tuple_cache_entries_in_use_bytes_(
        metrics->AddGauge("impala.tuple-cache.entries-in-use-bytes", 0)),
    tuple_cache_tombstones_in_use_(
        metrics->AddGauge("impala.tuple-cache.tombstones-in-use", 0)),
    tuple_cache_entry_size_stats_(metrics->RegisterMetric(
        new HistogramMetric(MetricDefs::Get("impala.tuple-cache.entry-sizes"),
            STATS_MAX_TUPLE_CACHE_ENTRY_SIZE, 3))) {}

Status TupleCacheMgr::Init() {
  if (cache_config_.empty()) {
    LOG(INFO) << "Tuple Cache is disabled.";
    return Status::OK();
  }

  // The expected form of the configuration string is: dir1:capacity
  // Example: /tmp/data1:1TB
  vector<string> all_cache_configs = Split(cache_config_, ":", SkipEmpty());
  if (all_cache_configs.size() != 2) {
    return Status(Substitute("Malformed data cache configuration $0", cache_config_));
  }

  // Parse the capacity string to make sure it's well-formed
  bool is_percent = false;
  int64_t capacity = ParseUtil::ParseMemSpec(all_cache_configs[1], &is_percent, 0);
  if (is_percent) {
    return Status(Substitute("Malformed tuple cache capacity configuration $0",
        all_cache_configs[1]));
  }
  // For normal execution (not backend tests), impose a minimum size on the
  // cache of 64MB.
  if (!TestInfo::is_be_test() && capacity < MIN_TUPLE_CACHE_CAPACITY_BYTES) {
    return Status(Substitute(
        "Tuple cache capacity $0 is less than the minimum allowed capacity $1",
        all_cache_configs[1], MIN_TUPLE_CACHE_CAPACITY_STR));
  }

  vector<string> cache_dirs = Split(all_cache_configs[0], ",", SkipEmpty());
  if (cache_dirs.size() > 1) {
    return Status(Substitute("Malformed tuple cache directory $0. "
        "The tuple cache only supports a single directory.", all_cache_configs[0]));
  }
  cache_dir_ = cache_dirs[0];

  // Verify the validity of the path specified.
  if (!FileSystemUtil::IsCanonicalPath(cache_dir_)) {
    return Status(Substitute("$0 is not a canonical path", cache_dir_));
  }
  RETURN_IF_ERROR(FileSystemUtil::VerifyIsDirectory(cache_dir_));

  // Verify we can create a file in the cache directory
  filesystem::path path =
      filesystem::path(cache_dir_) / Substitute("$0test", CACHE_FILE_PREFIX);
  RETURN_IF_ERROR(FileSystemUtil::CreateFile(path.string()));

  // Remove any existing cache files from the cache directory. This ensures that the
  // usage starts at zero. This also removes the test file we just wrote.
  RETURN_IF_ERROR(DeleteExistingFiles());

  // Check that there is enough space available for the specified cache size
  uint64_t available_bytes;
  RETURN_IF_ERROR(FileSystemUtil::GetSpaceAvailable(cache_dir_, &available_bytes));
  if (available_bytes < capacity) {
    string err = Substitute("Insufficient space for $0. Required $1. Only $2 is "
        "available", cache_dir_, PrettyPrinter::PrintBytes(capacity),
        PrettyPrinter::PrintBytes(available_bytes));
    LOG(ERROR) << err;
    return Status(err);
  }

  // Verify the cache eviction policy
  Cache::EvictionPolicy policy = Cache::ParseEvictionPolicy(eviction_policy_str_);
  if (policy != Cache::EvictionPolicy::LRU && policy != Cache::EvictionPolicy::LIRS) {
    return Status(Substitute("Unsupported tuple cache eviction policy: $0",
        eviction_policy_str_));
  }

  cache_.reset(NewCache(policy, capacity, "Tuple_Cache"));

  RETURN_IF_ERROR(cache_->Init());

  LOG(INFO) << "Tuple Cache initialized at " << cache_dir_
            << " with capacity " << PrettyPrinter::Print(capacity, TUnit::BYTES);
  enabled_ = true;
  return Status::OK();
}

// TupleCacheState tracks what operations can be performed on a cache entry,
// and is stored as part of a UniqueHandle. The diagram below outlines transitions
// between states and what operations are allowed. State transitions are atomic. State is
// represented by [TupleCacheState, IsAvailableForRead()]. Cache lookup, insert,
// erase all rely on internal locking of the private Cache object.
//
// Lookup(acquire_state=false): If an entry exists return it; else return an empty entry.
//                              IsAvailableForWrite() always returns false.
//
// Lookup(acquire_state=true): If an entry exists return it (IsAvailableForWrite()=false);
//                             else acquire creation_lock_ and check if an entry exists
//                             again, if found return it (IsAvailableForWrite()=false),
//                             else create a new entry (IsAvailableForWrite()=true).
//
//                          entry found
// Lookup(acquire_state=true)   --->      [ ... ] returns any of the states below
//          |
//          | entry absent: create new entry
//          v               CompleteWrite
//   [ IN_PROGRESS, false ]     --->      [ COMPLETE, true ]
//          |
//          | AbortWrite
//          v              tombstone=true
//   [ IN_PROGRESS, false ]     --->      [ TOMBSTONE, false ]
//          |
//          | tombstone=false
//          v
//   [ IN_PROGRESS, false ] Scheduled for eviction, will be deleted once ref count=0.
//

enum class TupleCacheState { IN_PROGRESS, TOMBSTONE, COMPLETE };

// An entry consists of a TupleCacheEntry followed by a C-string for the path.
struct TupleCacheEntry {
  std::atomic<TupleCacheState> state{TupleCacheState::IN_PROGRESS};
  size_t size = 0;
};

struct TupleCacheMgr::Handle {
  Cache::UniqueHandle cache_handle;
  bool is_writer = false;
};


void TupleCacheMgr::HandleDeleter::operator()(Handle* ptr) const { delete ptr; }

static uint8_t* getHandleData(const Cache* cache, TupleCacheMgr::Handle* handle) {
  DCHECK(handle->cache_handle != nullptr);
  return cache->Value(handle->cache_handle).mutable_data();
}

static TupleCacheState GetState(const Cache* cache, TupleCacheMgr::Handle* handle) {
  uint8_t* data = getHandleData(cache, handle);
  return reinterpret_cast<TupleCacheEntry*>(data)->state;
}

// Returns true if state was updated.
static bool UpdateState(const Cache* cache, TupleCacheMgr::Handle* handle,
    TupleCacheState requiredState, TupleCacheState newState) {
  uint8_t* data = getHandleData(cache, handle);
  return reinterpret_cast<TupleCacheEntry*>(data)->
      state.compare_exchange_strong(requiredState, newState);
}

static void UpdateSize(Cache* cache, TupleCacheMgr::Handle* handle, size_t size) {
  uint8_t* data = getHandleData(cache, handle);
  reinterpret_cast<TupleCacheEntry*>(data)->size = size;
}

static Cache::UniquePendingHandle CreateEntry(
    Cache* cache, const Slice& key, const string& path) {
  Cache::UniquePendingHandle pending =
      cache->Allocate(key, sizeof(TupleCacheEntry) + path.size() + 1);
  if (!pending) {
    return pending;
  }

  // buf is used to store a TupleCacheEntry, followed by the path.
  uint8_t* buf = cache->MutableValue(&pending);
  new (buf) TupleCacheEntry{};
  char* path_s = reinterpret_cast<char*>(buf + sizeof(TupleCacheEntry));
  memcpy(path_s, path.data(), path.size());
  // Null terminate because UniquePendingHandle doesn't provide access to size.
  path_s[path.size()] = '\0';

  return pending;
}

// If the entry exists, the Handle pins it so it doesn't go away, but the entry may be in
// any state (IN PROGRESS, TOMBSTONE, COMPLETE). If the entry doesn't exist and
// acquire_write is true, it's created with the state IN_PROGRESS.
TupleCacheMgr::UniqueHandle TupleCacheMgr::Lookup(
    const Slice& key, bool acquire_write) {
  if (!enabled_) return nullptr;

  UniqueHandle handle{new Handle()};
  if (Cache::UniqueHandle pos = cache_->Lookup(key); pos) {
    handle->cache_handle = move(pos);
  } else if (acquire_write) {
    lock_guard<mutex> guard(creation_lock_);
    // Retry lookup under the creation lock in case another thread added an entry.
    if (Cache::UniqueHandle pos = cache_->Lookup(key); pos) {
      handle->cache_handle = move(pos);
    } else {
      // Entry not found, create a new one.
      filesystem::path path =
          filesystem::path(cache_dir_) / (CACHE_FILE_PREFIX + key.ToString());
      Cache::UniquePendingHandle pending = CreateEntry(cache_.get(), key, path.string());
      if (UNLIKELY(!pending || debug_pos_ & DebugPos::FAIL_ALLOCATE)) {
        VLOG_FILE << "Tuple Cache: CreateEntry failed for " << path;
        return handle;
      }

      // Insert into the cache. If immediately evicted, evict_callback_ handles cleanup
      // and decrements the counter.
      tuple_cache_entries_in_use_->Increment(1);
      Cache::UniqueHandle chandle = cache_->Insert(move(pending), this);
      if (UNLIKELY(!chandle || debug_pos_ & DebugPos::FAIL_INSERT)) {
        // This shouldn't happen normally because the cache is recency-based and initial
        // handle is small.
        LOG(WARNING) << "Tuple Cache Entry was immediately evicted";
        return handle;
      }
      VLOG_FILE << "Tuple Cache Entry created for " << path;
      handle->cache_handle = move(chandle);
      handle->is_writer = true;
    }
  }

  return handle;
}

bool TupleCacheMgr::IsAvailableForRead(UniqueHandle& handle) const {
  if (!handle || !handle->cache_handle) return false;
  return TupleCacheState::COMPLETE == GetState(cache_.get(), handle.get());
}

bool TupleCacheMgr::IsAvailableForWrite(UniqueHandle& handle) const {
  if (!handle || !handle->cache_handle) return false;
  return handle->is_writer &&
      TupleCacheState::IN_PROGRESS == GetState(cache_.get(), handle.get());
}

void TupleCacheMgr::CompleteWrite(UniqueHandle handle, size_t size) {
  DCHECK(enabled_);
  DCHECK(handle != nullptr && handle->cache_handle != nullptr);
  DCHECK_LE(size, MaxSize());
  DCHECK_GE(size, 0);
  VLOG_FILE << "Tuple Cache: Complete " << GetPath(handle) << " (" << size << ")";
  CHECK(UpdateState(cache_.get(), handle.get(),
      TupleCacheState::IN_PROGRESS, TupleCacheState::COMPLETE));
  UpdateSize(cache_.get(), handle.get(), size);
  cache_->UpdateCharge(handle->cache_handle, size);
  tuple_cache_entries_in_use_bytes_->Increment(size);
  tuple_cache_entry_size_stats_->Update(size);
}

void TupleCacheMgr::AbortWrite(UniqueHandle handle, bool tombstone) {
  DCHECK(enabled_);
  DCHECK(handle != nullptr && handle->cache_handle != nullptr);
  if (tombstone) {
    VLOG_FILE << "Tuple Cache: Tombstone " << GetPath(handle);
    tuple_cache_tombstones_in_use_->Increment(1);
    CHECK(UpdateState(cache_.get(), handle.get(),
        TupleCacheState::IN_PROGRESS, TupleCacheState::TOMBSTONE));
  } else {
    // Remove the cache entry. Leaves state IN_PROGRESS so entry won't be reused until
    // successfully evicted.
    DCHECK(TupleCacheState::IN_PROGRESS == GetState(cache_.get(), handle.get()));
    cache_->Erase(cache_->Key(handle->cache_handle));
  }
}

const char* TupleCacheMgr::GetPath(UniqueHandle& handle) const {
  DCHECK(enabled_);
  DCHECK(handle != nullptr && handle->cache_handle != nullptr);
  uint8_t* data = getHandleData(cache_.get(), handle.get());
  return reinterpret_cast<const char*>(data + sizeof(TupleCacheEntry));
}

void TupleCacheMgr::EvictedEntry(Slice key, Slice value) {
  const TupleCacheEntry* entry = reinterpret_cast<const TupleCacheEntry*>(value.data());
  if (TupleCacheState::TOMBSTONE != entry->state) {
    DCHECK(tuple_cache_entries_evicted_ != nullptr);
    DCHECK(tuple_cache_entries_in_use_ != nullptr);
    DCHECK(tuple_cache_entries_in_use_bytes_ != nullptr);
    tuple_cache_entries_evicted_->Increment(1);
    tuple_cache_entries_in_use_->Increment(-1);
    tuple_cache_entries_in_use_bytes_->Increment(-entry->size);
    DCHECK_GE(tuple_cache_entries_in_use_->GetValue(), 0);
    DCHECK_GE(tuple_cache_entries_in_use_bytes_->GetValue(), 0);
  } else {
    DCHECK(tuple_cache_tombstones_in_use_ != nullptr);
    tuple_cache_tombstones_in_use_->Increment(-1);
    DCHECK_GE(tuple_cache_tombstones_in_use_->GetValue(), 0);
  }
  // Retrieve the path following TupleCacheEntry.
  value.remove_prefix(sizeof(TupleCacheEntry));
  value.truncate(value.size() - 1);
  VLOG_FILE << "Tuple Cache: Evict " << key << " at " << value.ToString();
  // Delete file on disk.
  kudu::Status status = kudu::Env::Default()->DeleteFile(value.ToString());
  if (!status.ok()) {
    LOG(WARNING) <<
        Substitute("Failed to unlink $0: $1", value.ToString(), status.ToString());
  }
}

Status TupleCacheMgr::DeleteExistingFiles() const {
  vector<string> entries;
  RETURN_IF_ERROR(FileSystemUtil::Directory::GetEntryNames(cache_dir_, &entries, 0,
      FileSystemUtil::Directory::EntryType::DIR_ENTRY_REG));
  for (const string& entry : entries) {
    if (entry.find(CACHE_FILE_PREFIX) == 0) {
      const string file_path = JoinPathSegments(cache_dir_, entry);
      KUDU_RETURN_IF_ERROR(kudu::Env::Default()->DeleteFile(file_path),
          Substitute("Failed to delete old cache file $0", file_path));
      LOG(INFO) << Substitute("Deleted old cache file $0", file_path);
    }
  }
  return Status::OK();
}

} // namespace impala
