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

#ifndef IMPALA_RUNTIME_DISK_IO_MGR_HANDLE_CACHE_H
#define IMPALA_RUNTIME_DISK_IO_MGR_HANDLE_CACHE_H

#include <array>
#include <list>
#include <map>
#include <memory>

#include <boost/thread/mutex.hpp>

#include "common/hdfs.h"
#include "common/status.h"
#include "util/aligned-new.h"
#include "util/spinlock.h"
#include "util/thread.h"

namespace impala {
namespace io {

class HdfsMonitor;

/// This abstract class is a small wrapper around the hdfsFile handle and the file system
/// instance which is needed to close the file handle. The handle incorporates
/// the last modified time of the file when it was opened. This is used to distinguish
/// between file handles for files that can be updated or overwritten.
/// This is used only through its subclasses, CachedHdfsFileHandle and
/// ExclusiveHdfsFileHandle.
class HdfsFileHandle {
 public:

  /// Destructor will close the file handle
  ~HdfsFileHandle();

  /// Init opens the file handle
  Status Init(HdfsMonitor* monitor);

  hdfsFile file() const { return hdfs_file_;  }
  int64_t mtime() const { return mtime_; }

 protected:
 HdfsFileHandle(const hdfsFS& fs, const std::string* fname, int64_t mtime)
    : fs_(fs), fname_(fname), mtime_(mtime) {}

 private:
  hdfsFS fs_;
  // fname_ has a limited lifetime. It is only valid from construction until Init().
  const std::string* fname_;
  hdfsFile hdfs_file_ = nullptr;
  int64_t mtime_;
};

/// CachedHdfsFileHandles are owned by the file handle cache and are used for no
/// other purpose.
class CachedHdfsFileHandle : public HdfsFileHandle {
 public:
  CachedHdfsFileHandle(const hdfsFS& fs, const std::string* fname, int64_t mtime);
  ~CachedHdfsFileHandle();
};

/// ExclusiveHdfsFileHandles are used for all purposes where a CachedHdfsFileHandle
/// is not appropriate.
class ExclusiveHdfsFileHandle : public HdfsFileHandle {
 public:
  ExclusiveHdfsFileHandle(const hdfsFS& fs, const std::string* fname, int64_t mtime)
    : HdfsFileHandle(fs, fname, mtime) {}
};

/// The FileHandleCache is a data structure that owns HdfsFileHandles to share between
/// threads. The HdfsFileHandles are hash partitioned across NUM_PARTITIONS partitions.
/// Each partition operates independently with its own locks, reducing contention
/// between concurrent threads. The `capacity` is split between the partitions and is
/// enforced independently.
///
/// Threads check out a file handle for exclusive access and return it when finished.
/// If the file handle is not already present in the cache or all file handles for this
/// file are checked out, the file handle is constructed and added to the cache.
/// The cache can contain multiple file handles for the same file. If a file handle
/// is checked out, it cannot be evicted from the cache. In this case, a cache can
/// exceed the specified capacity.
///
/// Some remote file systems such as S3 keep a connection as part of the file handle.
/// The file handle cache is not suitable for those systems, as the cache size can
/// exceed the limit on the number of concurrent connections. HDFS does not maintain
/// a connection in the file handle, so remote HDFS file handles do not have this
/// restriction.
///
/// If there is a file handle in the cache and the underlying file is deleted,
/// the file handle might keep the file from being deleted at the OS level. This can
/// take up disk space and impact correctness. To avoid this, the cache will evict any
/// file handle that has been unused for longer than threshold specified by
/// `unused_handle_timeout_secs`. Eviction is disabled when the threshold is 0.
///
/// TODO: The cache should also evict file handles more aggressively if the file handle's
/// mtime is older than the file's current mtime.
class FileHandleCache {
 public:
  /// Instantiates the cache with `capacity` split evenly across NUM_PARTITIONS
  /// partitions. If the capacity does not split evenly, then the capacity is rounded
  /// up. The cache will age out any file handle that is unused for
  /// `unused_handle_timeout_secs` seconds. Age out is disabled if this is set to zero.
  FileHandleCache(size_t capacity, size_t num_partitions,
      uint64_t unused_handle_timeout_secs, HdfsMonitor* hdfs_monitor);

  /// Destructor is only called for backend tests
  ~FileHandleCache();

  /// Starts up a thread that monitors the age of file handles and evicts any that
  /// exceed the limit.
  Status Init() WARN_UNUSED_RESULT;

  /// Get a file handle from the cache for the specified filename (fname) and
  /// last modification time (mtime). This will hash the filename to determine
  /// which partition to use for this file handle.
  ///
  /// If 'require_new_handle' is false and the partition contains an available handle,
  /// the handle is returned and cache_hit is set to true. Otherwise, the partition will
  /// try to construct a file handle and add it to the partition. On success, the new
  /// file handle will be returned with cache_hit set to false. On failure, nullptr will
  /// be returned. In either case, the partition may evict a file handle to make room
  /// for the new file handle.
  ///
  /// This obtains exclusive control over the returned file handle. It must be paired
  /// with a call to ReleaseFileHandle to release exclusive control.
  Status GetFileHandle(const hdfsFS& fs, std::string* fname,
      int64_t mtime, bool require_new_handle, CachedHdfsFileHandle** handle_out,
      bool* cache_hit) WARN_UNUSED_RESULT;

  /// Release the exclusive hold on the specified file handle (which was obtained
  /// by calling GetFileHandle). The cache may evict a file handle if the cache is
  /// above capacity. If 'destroy_handle' is true, immediately remove this handle
  /// from the cache.
  void ReleaseFileHandle(std::string* fname, CachedHdfsFileHandle* fh,
      bool destroy_handle);

 private:
  struct FileHandleEntry;
  typedef std::multimap<std::string, FileHandleEntry> MapType;

  struct LruListEntry {
    LruListEntry(typename MapType::iterator map_entry_in);
    typename MapType::iterator map_entry;
    uint64_t timestamp_seconds;
  };
  typedef std::list<LruListEntry> LruListType;

  struct FileHandleEntry {
    FileHandleEntry(std::unique_ptr<CachedHdfsFileHandle> fh_in, LruListType& lru_list)
    : fh(std::move(fh_in)), lru_entry(lru_list.end()) {}
    std::unique_ptr<CachedHdfsFileHandle> fh;

    /// in_use is true for a file handle checked out via GetFileHandle() that has not
    /// been returned via ReleaseFileHandle().
    bool in_use = false;

    /// Iterator to this element's location in the LRU list. This only points to a
    /// valid location when in_use is true. For error-checking, this is set to
    /// lru_list.end() when in_use is false.
    typename LruListType::iterator lru_entry;
  };

  /// Each partition operates independently, and thus has its own cache, LRU list,
  /// and corresponding lock. To avoid contention on the lock_ due to false sharing
  /// the partitions are aligned to cache line boundaries.
  struct FileHandleCachePartition : public CacheLineAligned {
    /// Protects access to cache and lru_list.
    SpinLock lock;

    /// Multimap from the file name to the file handles for that file. The cache
    /// can contain multiple file handles for the same file and some may have
    /// different mtimes if the file is being modified. All file handles are always
    /// owned by the cache.
    MapType cache;

    /// The LRU list only contains file handles that are not in use.
    LruListType lru_list;

    /// Maximum number of file handles in cache without evicting unused file handles.
    /// It is not a strict limit, and can be exceeded if all file handles are in use.
    size_t capacity;

    /// Current number of file handles in the cache
    size_t size;
  };

  /// Periodic check to evict unused file handles. Only executed by eviction_thread_.
  void EvictHandlesLoop();
  static const int64_t EVICT_HANDLES_PERIOD_MS = 1000;

  /// If the partition is above its capacity, evict the oldest unused file handles to
  /// enforce the capacity.
  void EvictHandles(FileHandleCachePartition& p);

  std::vector<FileHandleCachePartition> cache_partitions_;

  /// Maximum time before an unused file handle is aged out of the cache.
  /// Aging out is disabled if this is set to 0.
  uint64_t unused_handle_timeout_secs_;

  /// Thread to check for unused file handles to evict. This thread will exit when
  /// the shut_down_promise_ is set.
  std::unique_ptr<Thread> eviction_thread_;
  Promise<bool> shut_down_promise_;

  /// Thread pool used to implement timeouts for HDFS operations
  HdfsMonitor* hdfs_monitor_;
};
}
}

#endif
