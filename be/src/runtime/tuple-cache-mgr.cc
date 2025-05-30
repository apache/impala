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

#include "common/constant-strings.h"
#include "common/logging.h"
#include "exec/tuple-file-reader.h"
#include "exec/tuple-text-file-reader.h"
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
DEFINE_string(tuple_cache_debug_dump_dir, "",
    "Directory for dumping the intermediate query result tuples for debugging purpose.");

DEFINE_uint32(tuple_cache_sync_pool_size, 10,
    "(Advanced) Size of the thread pool syncing cache files to disk asynchronously. "
    "If set to 0, cache files are flushed sychronously.");
DEFINE_uint32(tuple_cache_sync_pool_queue_depth, 1000,
    "(Advanced) Maximum queue depth for the thread pool syncing cache files to disk");

static const string OUTSTANDING_WRITE_LIMIT_MSG =
  "(Advanced) Limit on the size of outstanding tuple cache writes. " +
  Substitute(MEM_UNITS_HELP_MSG, "the process memory limit");
DEFINE_string(tuple_cache_outstanding_write_limit, "1GB",
    OUTSTANDING_WRITE_LIMIT_MSG.c_str());
DEFINE_uint32(tuple_cache_outstanding_write_chunk_bytes, 128 * 1024,
    "(Advanced) Chunk size for incrementing the outstanding tuple cache write size");

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
static const char* DUBUG_DUMP_SUB_DIR_NAME = "tuple-cache-debug-dump";

static string ConstructTupleCacheDebugDumpPath() {
  // Construct and return the path for debug dumping the tuple cache if configured.
  string& cache_debug_dump_dir = FLAGS_tuple_cache_debug_dump_dir;
  if (cache_debug_dump_dir != "") {
    filesystem::path path(cache_debug_dump_dir);
    path /= DUBUG_DUMP_SUB_DIR_NAME;
    // Remove and recreate the subdirectory if it exists.
    Status cr_status = FileSystemUtil::RemoveAndCreateDirectory(path.string());
    if (cr_status.ok()) {
      LOG(INFO) << "Created tuple cache debug dump path in " << path.string();
      return path.string();
    }
    LOG(WARNING) << "Unable to create directory for tuple cache dumping: "
                 << cr_status.GetDetail();
  }
  return string();
}

TupleCacheMgr::TupleCacheMgr(MetricGroup* metrics)
  : TupleCacheMgr(FLAGS_tuple_cache, FLAGS_tuple_cache_eviction_policy, metrics,
        /* debug_pos */ 0, FLAGS_tuple_cache_sync_pool_size,
        FLAGS_tuple_cache_sync_pool_queue_depth,
        FLAGS_tuple_cache_outstanding_write_limit,
        FLAGS_tuple_cache_outstanding_write_chunk_bytes) {}

TupleCacheMgr::TupleCacheMgr(
    string cache_config, string eviction_policy_str, MetricGroup* metrics,
    uint8_t debug_pos, uint32_t sync_pool_size, uint32_t sync_pool_queue_depth,
    string outstanding_write_limit_str, uint32_t outstanding_write_chunk_bytes)
  : cache_config_(move(cache_config)),
    eviction_policy_str_(move(eviction_policy_str)),
    outstanding_write_limit_str_(move(outstanding_write_limit_str)),
    cache_debug_dump_dir_(ConstructTupleCacheDebugDumpPath()),
    debug_pos_(debug_pos),
    sync_pool_size_(sync_pool_size),
    sync_pool_queue_depth_(sync_pool_queue_depth),
    outstanding_write_chunk_bytes_(outstanding_write_chunk_bytes),
    tuple_cache_hits_(metrics->AddCounter("impala.tuple-cache.hits", 0)),
    tuple_cache_misses_(metrics->AddCounter("impala.tuple-cache.misses", 0)),
    tuple_cache_skipped_(metrics->AddCounter("impala.tuple-cache.skipped", 0)),
    tuple_cache_halted_(metrics->AddCounter("impala.tuple-cache.halted", 0)),
    tuple_cache_backpressure_halted_(
        metrics->AddCounter("impala.tuple-cache.backpressure-halted", 0)),
    tuple_cache_entries_evicted_(
        metrics->AddCounter("impala.tuple-cache.entries-evicted", 0)),
    tuple_cache_failed_sync_(metrics->AddCounter("impala.tuple-cache.failed-syncs", 0)),
    tuple_cache_dropped_sync_(metrics->AddCounter("impala.tuple-cache.dropped-syncs", 0)),
    tuple_cache_entries_in_use_(
        metrics->AddGauge("impala.tuple-cache.entries-in-use", 0)),
    tuple_cache_entries_in_use_bytes_(
        metrics->AddGauge("impala.tuple-cache.entries-in-use-bytes", 0)),
    tuple_cache_tombstones_in_use_(
        metrics->AddGauge("impala.tuple-cache.tombstones-in-use", 0)),
    tuple_cache_outstanding_writes_bytes_(
        metrics->AddGauge("impala.tuple-cache.outstanding-writes-bytes", 0)),
    tuple_cache_entry_size_stats_(metrics->RegisterMetric(
        new HistogramMetric(MetricDefs::Get("impala.tuple-cache.entry-sizes"),
            STATS_MAX_TUPLE_CACHE_ENTRY_SIZE, 3))) {}

Status TupleCacheMgr::Init(int64_t process_bytes_limit) {
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

  // The outstanding write limit can either be a specific value, or it can be a
  // percentage of the process bytes limit. If the process bytes limit is zero,
  // a percentage is not allowed.
  outstanding_write_limit_ = ParseUtil::ParseMemSpec(outstanding_write_limit_str_,
      &is_percent, process_bytes_limit);
  if (outstanding_write_limit_ <= 0) {
    CLEAN_EXIT_WITH_ERROR(
        Substitute("Invalid tuple cache outstanding write limit configuration: $0.",
            FLAGS_tuple_cache_outstanding_write_limit));
  }

  // Setting sync_pool_size == 0 results in synchronous flushing to disk. This is
  // mainly used for backend tests
  if (sync_pool_size_ > 0) {
    sync_thread_pool_.reset(new ThreadPool<string>("tuple-cache-mgr", "sync-worker",
        sync_pool_size_, sync_pool_queue_depth_,
        [this] (int thread_id, const string& filename) {
          this->SyncFileToDisk(filename);
        }));
    RETURN_IF_ERROR(sync_thread_pool_->Init());
  }

  cache_.reset(NewCache(policy, capacity, "Tuple_Cache"));

  RETURN_IF_ERROR(cache_->Init());

  LOG(INFO) << "Tuple Cache initialized at " << cache_dir_
            << " with capacity " << PrettyPrinter::Print(capacity, TUnit::BYTES)
            << " and outstanding write limit: "
            << PrettyPrinter::Print(outstanding_write_limit_, TUnit::BYTES);
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
//                         entry found
// Lookup(acquire_state=true) ---> [ ... ] returns any of the states below
//          |
//          | entry absent: create new entry
//          v            CompleteWrite                     SyncFileToDisk
//   [ IN_PROGRESS, false ]   ---> [ COMPLETE_UNSYNCED, true ] ---> [ COMPLETE, true ]
//          |
//          | AbortWrite
//          v            tombstone=true
//   [ IN_PROGRESS, false ]   ---> [ TOMBSTONE, false ]
//          |
//          | tombstone=false
//          v
//   [ IN_PROGRESS, false ] Scheduled for eviction, will be deleted once ref count=0.
//

enum class TupleCacheState { IN_PROGRESS, TOMBSTONE, COMPLETE_UNSYNCED, COMPLETE };

// An entry consists of a TupleCacheEntry followed by a C-string for the path.
struct TupleCacheEntry {
  std::atomic<TupleCacheState> state{TupleCacheState::IN_PROGRESS};
  // Charge in the cache when there is a file associated with this entry. This is zero for
  // TOMBSTONE and IN_PROGRESS before the first UpdateWriteSize call, but those states
  // still have a base charge in the cache. During IN_PROGRESS, this is a reservation that
  // exceeds the current size of the file.
  size_t charge = 0;
};

struct TupleCacheMgr::Handle {
  Cache::UniqueHandle cache_handle;
  bool is_writer = false;
  // Minimum charge to use if this entry becomes a TOMBSTONE
  size_t base_charge = 0;
};

void TupleCacheMgr::HandleDeleter::operator()(Handle* ptr) const { delete ptr; }

static uint8_t* getHandleData(const Cache* cache, TupleCacheMgr::Handle* handle) {
  DCHECK(handle->cache_handle != nullptr);
  return cache->Value(handle->cache_handle).mutable_data();
}

TupleCacheState TupleCacheMgr::GetState(TupleCacheMgr::Handle* handle) const {
  uint8_t* data = getHandleData(cache_.get(), handle);
  return reinterpret_cast<TupleCacheEntry*>(data)->state;
}

// Returns true if state was updated.
bool TupleCacheMgr::UpdateState(TupleCacheMgr::Handle* handle,
    TupleCacheState requiredState, TupleCacheState newState) {
  uint8_t* data = getHandleData(cache_.get(), handle);
  return reinterpret_cast<TupleCacheEntry*>(data)->
      state.compare_exchange_strong(requiredState, newState);
}

size_t TupleCacheMgr::GetCharge(TupleCacheMgr::Handle* handle) const {
  uint8_t* data = getHandleData(cache_.get(), handle);
  return reinterpret_cast<TupleCacheEntry*>(data)->charge;
}

void TupleCacheMgr::UpdateWriteSize(TupleCacheMgr::Handle* handle,
    size_t charge) {
  uint8_t* data = getHandleData(cache_.get(), handle);
  // We can only adjust the cache charge while an entry is IN_PROGRESS
  DCHECK(TupleCacheState::IN_PROGRESS == GetState(handle));
  TupleCacheEntry* entry = reinterpret_cast<TupleCacheEntry*>(data);
  int64_t diff = charge - entry->charge;
  entry->charge = charge;
  cache_->UpdateCharge(handle->cache_handle, charge);
  if (diff < 0) {
    DCHECK_LE(-diff, tuple_cache_outstanding_writes_bytes_->GetValue());
  }
  tuple_cache_outstanding_writes_bytes_->Increment(diff);
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
// any state (IN PROGRESS, TOMBSTONE, COMPLETE_UNSYNCED, COMPLETE). If the entry doesn't
// exist and acquire_write is true, it's created with the state IN_PROGRESS.
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
      handle->base_charge = sizeof(TupleCacheEntry) + path.size() + 1;
    }
  }

  return handle;
}

bool TupleCacheMgr::IsAvailableForRead(UniqueHandle& handle) const {
  if (!handle || !handle->cache_handle) return false;
  TupleCacheState state = GetState(handle.get());
  return TupleCacheState::COMPLETE_UNSYNCED == state ||
      TupleCacheState::COMPLETE == state;
}

bool TupleCacheMgr::IsAvailableForWrite(UniqueHandle& handle) const {
  if (!handle || !handle->cache_handle) return false;
  return handle->is_writer && TupleCacheState::IN_PROGRESS == GetState(handle.get());
}

void TupleCacheMgr::CompleteWrite(UniqueHandle handle, size_t size) {
  DCHECK(enabled_);
  DCHECK(handle != nullptr && handle->cache_handle != nullptr);
  DCHECK(handle->is_writer);
  DCHECK_LE(size, MaxSize());
  DCHECK_GE(size, 0);
  if (sync_pool_size_ > 0 &&
      sync_thread_pool_->GetQueueSize() >= sync_pool_queue_depth_) {
    // The sync_thread_pool_ has reached its max queue size. This should almost never
    // happen, as the outstanding writes limit should kick in before this is overwhelmed.
    // If it does happen, bail out.
    AbortWrite(move(handle), false);
    tuple_cache_dropped_sync_->Increment(1);
    return;
  }
  VLOG_FILE << "Tuple Cache: Complete " << GetPath(handle) << " (" << size << ")";
  UpdateWriteSize(handle.get(), size);
  CHECK(UpdateState(handle.get(),
      TupleCacheState::IN_PROGRESS, TupleCacheState::COMPLETE_UNSYNCED));
  tuple_cache_entries_in_use_bytes_->Increment(size);
  tuple_cache_entry_size_stats_->Update(size);
  // When the sync_pool_size_ is 0, there is no thread pool and this does the sync
  // directly. This is used for backend tests to avoid race conditions.
  if (sync_pool_size_ > 0) {
    // Offer the cache key to the thread pool.
    bool success = sync_thread_pool_->Offer(cache_->Key(handle->cache_handle).ToString());
    if (!success) {
      // The queue is full, so evict this entry
      VLOG_FILE << "Tuple Cache: Sync thread pool queue full. Evicting "
                << GetPath(handle);
      cache_->Erase(cache_->Key(handle->cache_handle));
      tuple_cache_dropped_sync_->Increment(1);
    }
  } else {
    SyncFileToDisk(cache_->Key(handle->cache_handle).ToString());
  }
}

void TupleCacheMgr::AbortWrite(UniqueHandle handle, bool tombstone) {
  DCHECK(enabled_);
  DCHECK(handle != nullptr && handle->cache_handle != nullptr);
  DCHECK(handle->is_writer);
  if (tombstone) {
    VLOG_FILE << "Tuple Cache: Tombstone " << GetPath(handle);
    tuple_cache_tombstones_in_use_->Increment(1);
    // We update the write size to 0 to remove the existing cache charge
    // (and decrement the outstanding writes counter)
    UpdateWriteSize(handle.get(), 0);
    CHECK(UpdateState(handle.get(),
        TupleCacheState::IN_PROGRESS, TupleCacheState::TOMBSTONE));
    // We want the tombstone cache entry to have a base charge, so set that now
    // (without counting towards the outstanding writes).
    cache_->UpdateCharge(handle->cache_handle, handle->base_charge);
  } else {
    // Remove the cache entry. Leaves state IN_PROGRESS so entry won't be reused until
    // successfully evicted.
    DCHECK(TupleCacheState::IN_PROGRESS == GetState(handle.get()));
    cache_->Erase(cache_->Key(handle->cache_handle));
  }
}

Status TupleCacheMgr::RequestWriteSize(UniqueHandle* handle, size_t new_size) {
  // The handle better be from a writer
  DCHECK((*handle)->is_writer);

  uint8_t* data = getHandleData(cache_.get(), handle->get());
  size_t cur_charge = reinterpret_cast<TupleCacheEntry*>(data)->charge;
  if (new_size > cur_charge) {
    // Need to increase the charge, which can fail
    // 1. There is a maximum size for any given entry
    // 2. There is a maximum amount of outstanding writes (i.e. dirty buffers)
    // The chunk size limits the frequency of incrementing the counter in the cache
    // itself. The chunk size is disabled for unit tests to have exact counter values.
    // The chunk size does not impact enforcement of the maximum entry size.

    // An individual entry cannot exceed the MaxSize()
    if (new_size > MaxSize()) {
      tuple_cache_halted_->Increment(1);
      return Status(TErrorCode::TUPLE_CACHE_ENTRY_SIZE_LIMIT_EXCEEDED, MaxSize());
    }

    size_t new_charge = new_size;
    if (outstanding_write_chunk_bytes_ != 0) {
      new_charge = ((new_size / outstanding_write_chunk_bytes_) + 1) *
          outstanding_write_chunk_bytes_;
      // The chunk size should not change the behavior of the MaxSize(), so limit the
      // new_charge to MaxSize() if it would otherwise exceed it.
      if (new_charge > MaxSize()) {
        new_charge = MaxSize();
      }
    }
    int64_t diff = new_charge - cur_charge;
    DCHECK_GT(new_charge, cur_charge);
    DCHECK_GE(new_charge, new_size);

    // Limit the total outstanding writes to avoid excessive dirty buffers for the OS
    if (tuple_cache_outstanding_writes_bytes_->GetValue() + diff >
        outstanding_write_limit_) {
      tuple_cache_backpressure_halted_->Increment(1);
      return Status(TErrorCode::TUPLE_CACHE_OUTSTANDING_WRITE_LIMIT_EXCEEDED,
          outstanding_write_limit_);
    }
    UpdateWriteSize(handle->get(), new_charge);
  }
  return Status::OK();
}

const char* TupleCacheMgr::GetPath(UniqueHandle& handle) const {
  DCHECK(enabled_);
  DCHECK(handle != nullptr && handle->cache_handle != nullptr);
  uint8_t* data = getHandleData(cache_.get(), handle.get());
  return reinterpret_cast<const char*>(data + sizeof(TupleCacheEntry));
}

Status TupleCacheMgr::CreateDebugDumpSubdir(const string& sub_dir) {
  DCHECK(!sub_dir.empty());
  bool path_exists = false;
  std::lock_guard<std::mutex> l(debug_dump_subdir_lock_);
  // Try to create the subdir if it doesn't already exist.
  RETURN_IF_ERROR(FileSystemUtil::PathExists(sub_dir, &path_exists));
  if (!path_exists) {
    RETURN_IF_ERROR(FileSystemUtil::CreateDirectory(sub_dir));
  }
  return Status::OK();
}

string TupleCacheMgr::GetDebugDumpPath(
    const string& sub_dir, const string& file_name, string* sub_dir_full_path) const {
  filesystem::path full_path(cache_debug_dump_dir_);
  full_path /= sub_dir;
  if (sub_dir_full_path != nullptr) *sub_dir_full_path = full_path.string();
  full_path /= file_name;
  return full_path.string();
}

void TupleCacheMgr::StoreMetadataForTupleCache(
    const string& cache_key, const string& fragment_id) {
  std::lock_guard<std::mutex> l(debug_dump_lock_);
  DebugDumpCacheMetaData& cache = debug_dump_caches_metadata_[cache_key];
  cache.fragment_id = fragment_id;
}

string TupleCacheMgr::GetFragmentIdForTupleCache(const string& cache_key) {
  std::lock_guard<std::mutex> l(debug_dump_lock_);
  auto iter = debug_dump_caches_metadata_.find(cache_key);
  if (iter == debug_dump_caches_metadata_.end()) {
    return string();
  }
  return iter->second.fragment_id;
}

void TupleCacheMgr::RemoveMetadataForTupleCache(const string& cache_key) {
  std::lock_guard<std::mutex> l(debug_dump_lock_);
  auto it = debug_dump_caches_metadata_.find(cache_key);
  if (it != debug_dump_caches_metadata_.end()) {
    debug_dump_caches_metadata_.erase(it);
  }
}

void TupleCacheMgr::EvictedEntry(Slice key, Slice value) {
  const TupleCacheEntry* entry = reinterpret_cast<const TupleCacheEntry*>(value.data());
  if (TupleCacheState::TOMBSTONE != entry->state) {
    DCHECK(tuple_cache_entries_evicted_ != nullptr);
    DCHECK(tuple_cache_entries_in_use_ != nullptr);
    DCHECK(tuple_cache_entries_in_use_bytes_ != nullptr);
    tuple_cache_entries_evicted_->Increment(1);
    tuple_cache_entries_in_use_->Increment(-1);
    DCHECK_GE(tuple_cache_entries_in_use_->GetValue(), 0);
    // entries_in_use_bytes is incremented only when the entry reaches the
    // COMPLETE_UNSYNCED state
    if (TupleCacheState::COMPLETE_UNSYNCED == entry->state ||
        TupleCacheState::COMPLETE == entry->state) {
      tuple_cache_entries_in_use_bytes_->Increment(-entry->charge);
      DCHECK_GE(tuple_cache_entries_in_use_bytes_->GetValue(), 0);
    }
    // Outstanding write bytes are accumulated during IN_PROGRESS, and remain set until
    // the transition from COMPLETE_UNSYNCED to COMPLETE.
    if (TupleCacheState::COMPLETE_UNSYNCED == entry->state ||
        TupleCacheState::IN_PROGRESS == entry->state) {
      DCHECK(tuple_cache_outstanding_writes_bytes_ != nullptr);
      DCHECK_GE(tuple_cache_outstanding_writes_bytes_->GetValue(), entry->charge);
      tuple_cache_outstanding_writes_bytes_->Increment(-entry->charge);
    }
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
  // If correctness checking is enabled, remove the associated metadata as well.
  if (DebugDumpEnabled()) {
    string key_str(reinterpret_cast<const char*>(key.data()), key.size());
    RemoveMetadataForTupleCache(key_str);
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

void TupleCacheMgr::SyncFileToDisk(const string& cache_key) {
  Cache::UniqueHandle pos = cache_->Lookup(cache_key);
  // The entry can be evicted while waiting to be synced to disk. If the entry no longer
  // exists, there is nothing to do.
  if (pos == nullptr) return;
  UniqueHandle handle{new Handle()};
  handle->cache_handle = move(pos);
  // If the entry has a state other than COMPLETE_UNSYNCED, it could have been
  // evicted and recreated. There is nothing to do.
  if (TupleCacheState::COMPLETE_UNSYNCED != GetState(handle.get())) {
    return;
  }
  bool success = true;
  // Some unit tests don't create a real file when testing the TupleCacheMgr, so
  // only do the sync if there is a backing file
  bool has_backing_file = !(debug_pos_ & DebugPos::NO_FILES);
  if (has_backing_file) {
    // Open the cache file associated with this key, then call Sync() on it, and
    // close it.
    std::string file_path = GetPath(handle);
    std::unique_ptr<kudu::RWFile> file_to_sync;
    kudu::RWFileOptions opts;
    opts.mode = kudu::Env::OpenMode::MUST_EXIST;
    kudu::Status s = kudu::Env::Default()->NewRWFile(opts, file_path, &file_to_sync);
    if (!s.ok()) {
      LOG(WARNING) << Substitute("SyncFileToDisk: Failed to open file $0: $1", file_path,
          s.ToString());
      success = false;
    } else {
      s = file_to_sync->Sync();
      if (!s.ok()) {
        LOG(WARNING) << Substitute("SyncFileToDisk: Failed to sync file $0: $1",
            file_path, s.ToString());
        success = false;
      }
      // Close the file even if Sync() fails
      s = file_to_sync->Close();
      if (!s.ok()) {
        LOG(WARNING) << Substitute("SyncFileToDisk: Failed to close file $0: $1",
            file_path, s.ToString());
        success = false;
      }
    }
  }
  if (success) {
    bool update_succeeded = UpdateState(handle.get(),
        TupleCacheState::COMPLETE_UNSYNCED, TupleCacheState::COMPLETE);
    if (update_succeeded) {
      tuple_cache_outstanding_writes_bytes_->Increment(-GetCharge(handle.get()));
    }
    // Only crash for a failed state change on debug builds. The sync completed
    // and the state change doesn't really impact external behavior. It isn't
    // worth crashing on a release build.
    DCHECK(update_succeeded);
  } else {
    // In case of any error, erase this cache entry
    VLOG_FILE << "Tuple Cache: SyncFileToDisk failed. Evicting " << GetPath(handle);
    cache_->Erase(cache_->Key(handle->cache_handle));
    tuple_cache_failed_sync_->Increment(1);
  }
}
} // namespace impala
