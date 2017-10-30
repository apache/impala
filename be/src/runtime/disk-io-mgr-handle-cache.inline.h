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

#include "runtime/disk-io-mgr-handle-cache.h"
#include "util/hash-util.h"
#include "util/time.h"

#ifndef IMPALA_RUNTIME_DISK_IO_MGR_HANDLE_CACHE_INLINE_H
#define IMPALA_RUNTIME_DISK_IO_MGR_HANDLE_CACHE_INLINE_H

namespace impala {

HdfsFileHandle::HdfsFileHandle(const hdfsFS& fs, const char* fname,
    int64_t mtime)
    : fs_(fs), hdfs_file_(hdfsOpenFile(fs, fname, O_RDONLY, 0, 0, 0)), mtime_(mtime) {
  ImpaladMetrics::IO_MGR_NUM_CACHED_FILE_HANDLES->Increment(1L);
  VLOG_FILE << "hdfsOpenFile() file=" << fname << " fid=" << hdfs_file_;
}

HdfsFileHandle::~HdfsFileHandle() {
  if (hdfs_file_ != nullptr && fs_ != nullptr) {
    ImpaladMetrics::IO_MGR_NUM_CACHED_FILE_HANDLES->Increment(-1L);
    VLOG_FILE << "hdfsCloseFile() fid=" << hdfs_file_;
    hdfsCloseFile(fs_, hdfs_file_);
  }
  fs_ = nullptr;
  hdfs_file_ = nullptr;
}

template <size_t NUM_PARTITIONS>
  FileHandleCache<NUM_PARTITIONS>::FileHandleCache(size_t capacity,
      uint64_t unused_handle_timeout_secs)
  : unused_handle_timeout_secs_(unused_handle_timeout_secs) {
  DCHECK_GT(NUM_PARTITIONS, 0);
  size_t remainder = capacity % NUM_PARTITIONS;
  size_t base_capacity = capacity / NUM_PARTITIONS;
  size_t partition_capacity = (remainder > 0 ? base_capacity + 1 : base_capacity);
  for (FileHandleCachePartition& p : cache_partitions_) {
    p.size = 0;
    p.capacity = partition_capacity;
  }
}

template <size_t NUM_PARTITIONS>
FileHandleCache<NUM_PARTITIONS>::LruListEntry::LruListEntry(
    typename MapType::iterator map_entry_in)
     : map_entry(map_entry_in), timestamp_seconds(MonotonicSeconds()) {}

template <size_t NUM_PARTITIONS>
FileHandleCache<NUM_PARTITIONS>::~FileHandleCache() {
  shut_down_promise_.Set(true);
  if (eviction_thread_ != nullptr) eviction_thread_->Join();
}

template <size_t NUM_PARTITIONS>
Status FileHandleCache<NUM_PARTITIONS>::Init() {
  return Thread::Create("disk-io-mgr-handle-cache", "File Handle Timeout",
      &FileHandleCache<NUM_PARTITIONS>::EvictHandlesLoop, this, &eviction_thread_);
}

template <size_t NUM_PARTITIONS>
HdfsFileHandle* FileHandleCache<NUM_PARTITIONS>::GetFileHandle(
    const hdfsFS& fs, std::string* fname, int64_t mtime, bool require_new_handle,
    bool* cache_hit) {
  // Hash the key and get appropriate partition
  int index = HashUtil::Hash(fname->data(), fname->size(), 0) % NUM_PARTITIONS;
  FileHandleCachePartition& p = cache_partitions_[index];
  boost::lock_guard<SpinLock> g(p.lock);
  pair<typename MapType::iterator, typename MapType::iterator> range =
    p.cache.equal_range(*fname);

  // If this requires a new handle, skip to the creation codepath. Otherwise,
  // find an unused entry with the same mtime
  FileHandleEntry* ret_elem = nullptr;
  if (!require_new_handle) {
    while (range.first != range.second) {
      FileHandleEntry* elem = &range.first->second;
      if (!elem->in_use && elem->fh->mtime() == mtime) {
        // This element is currently in the lru_list, which means that lru_entry must
        // be an iterator pointing into the lru_list.
        DCHECK(elem->lru_entry != p.lru_list.end());
        // Remove the element from the lru_list and designate that it is not on
        // the lru_list by resetting its iterator to point to the end of the list.
        p.lru_list.erase(elem->lru_entry);
        elem->lru_entry = p.lru_list.end();
        ret_elem = elem;
        *cache_hit = true;
        break;
      }
      ++range.first;
    }
  }

  // There was no entry that was free or caller asked for a new handle
  if (!ret_elem) {
    *cache_hit = false;
    // Create a new entry and move it into the map
    HdfsFileHandle* new_fh = new HdfsFileHandle(fs, fname->data(), mtime);
    if (!new_fh->ok()) {
      delete new_fh;
      return nullptr;
    }
    FileHandleEntry entry(new_fh, p.lru_list);
    typename MapType::iterator new_it = p.cache.emplace_hint(range.second,
        *fname, std::move(entry));
    ret_elem = &new_it->second;
    ++p.size;
    if (p.size > p.capacity) EvictHandles(p);
  }

  DCHECK(ret_elem->fh.get() != nullptr);
  DCHECK(!ret_elem->in_use);
  ret_elem->in_use = true;
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(1L);
  return ret_elem->fh.get();
}

template <size_t NUM_PARTITIONS>
void FileHandleCache<NUM_PARTITIONS>::ReleaseFileHandle(std::string* fname,
    HdfsFileHandle* fh, bool destroy_handle) {
  DCHECK(fh != nullptr);
  // Hash the key and get appropriate partition
  int index = HashUtil::Hash(fname->data(), fname->size(), 0) % NUM_PARTITIONS;
  FileHandleCachePartition& p = cache_partitions_[index];
  boost::lock_guard<SpinLock> g(p.lock);
  pair<typename MapType::iterator, typename MapType::iterator> range =
    p.cache.equal_range(*fname);

  // TODO: This can be optimized by maintaining some state in the file handle about
  // its location in the map.
  typename MapType::iterator release_it = range.first;
  while (release_it != range.second) {
    FileHandleEntry* elem = &release_it->second;
    if (elem->fh.get() == fh) break;
    ++release_it;
  }
  DCHECK(release_it != range.second);

  // This file handle is no longer referenced
  FileHandleEntry* release_elem = &release_it->second;
  DCHECK(release_elem->in_use);
  release_elem->in_use = false;
  ImpaladMetrics::IO_MGR_NUM_FILE_HANDLES_OUTSTANDING->Increment(-1L);
  if (destroy_handle) {
    --p.size;
    p.cache.erase(release_it);
    return;
  }
  // Hdfs can use some memory for readahead buffering. Calling unbuffer reduces
  // this buffering so that the file handle takes up less memory when in the cache.
  // If unbuffering is not supported, then hdfsUnbufferFile() will return a non-zero
  // return code, and we close the file handle and remove it from the cache.
  if (hdfsUnbufferFile(release_elem->fh->file()) == 0) {
    // This FileHandleEntry must not be in the lru list already, because it was
    // in use. Verify this by checking that the lru_entry is pointing to the end,
    // which cannot be true for any element in the lru list.
    DCHECK(release_elem->lru_entry == p.lru_list.end());
    // Add this to the lru list, establishing links in both directions.
    // The FileHandleEntry has an iterator to the LruListEntry and the
    // LruListEntry has an iterator to the location of the FileHandleEntry in
    // the cache.
    release_elem->lru_entry = p.lru_list.emplace(p.lru_list.end(), release_it);
    if (p.size > p.capacity) EvictHandles(p);
  } else {
    VLOG_FILE << "FS does not support file handle unbuffering, closing file="
              << fname;
    --p.size;
    p.cache.erase(release_it);
  }
}

template <size_t NUM_PARTITIONS>
void FileHandleCache<NUM_PARTITIONS>::EvictHandlesLoop() {
  while (true) {
    for (FileHandleCachePartition& p : cache_partitions_) {
      boost::lock_guard<SpinLock> g(p.lock);
      EvictHandles(p);
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

template <size_t NUM_PARTITIONS>
void FileHandleCache<NUM_PARTITIONS>::EvictHandles(
    FileHandleCache<NUM_PARTITIONS>::FileHandleCachePartition& p) {
  uint64_t now = MonotonicSeconds();
  uint64_t oldest_allowed_timestamp =
      now > unused_handle_timeout_secs_ ? now - unused_handle_timeout_secs_ : 0;
  while (p.lru_list.size() > 0) {
    // Peek at the oldest element
    LruListEntry oldest_entry = p.lru_list.front();
    typename MapType::iterator oldest_entry_map_it = oldest_entry.map_entry;
    uint64_t oldest_entry_timestamp = oldest_entry.timestamp_seconds;
    // If the oldest element does not need to be aged out and the cache is not over
    // capacity, then we are done and there is nothing to evict.
    if (p.size <= p.capacity && (unused_handle_timeout_secs_ == 0 ||
        oldest_entry_timestamp >= oldest_allowed_timestamp)) {
      return;
    }
    // Evict the oldest element
    DCHECK(!oldest_entry_map_it->second.in_use);
    p.cache.erase(oldest_entry_map_it);
    p.lru_list.pop_front();
    --p.size;
  }
}

}
#endif
