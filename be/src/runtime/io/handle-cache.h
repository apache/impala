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

#include <array>
#include <list>
#include <map>
#include <memory>

#include "common/hdfs.h"
#include "common/status.h"
#include "util/aligned-new.h"
#include "util/lru-multi-cache.h"
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
/// Threads check out a file handle for exclusive access, released automatically by RAII
/// accessor. If the file handle is not already present in the cache or all file handles
/// for this file are checked out, the file handle is emplaced in the cache. The cache can
/// contain multiple file handles for the same file. If a file handle is checked out, it
/// cannot be evicted from the cache. In this case, a cache can exceed the specified
/// capacity.
///
/// Remote file systems could keep a connection as part of the file handle without support
/// for unbuffering. The file handle cache is not suitable for those systems, as the cache
/// size can exceed the limit on the number of concurrent connections. HDFS does not
/// maintain a connection in the file handle, S3A client supports unbuffering since
/// IMPALA-8428, so those do not have this restriction.
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
 private:
  /// Each partition operates independently, and thus has its own thread-safe cache.
  /// To avoid contention on the lock_ due to false sharing the partitions are
  /// aligned to cache line boundaries.
  struct FileHandleCachePartition : public CacheLineAligned {
    // Cache key is a pair of filename and mtime
    // Using std::pair to spare boilerplate of hash function
    typedef LruMultiCache<std::pair<std::string, int64_t>, CachedHdfsFileHandle>
        CacheType;
    CacheType cache;
  };

 public:
  /// RAII accessor built over LruMultiCache::Accessor to handle metrics and unbuffering.
  /// Composition is used instead of inheritance to support the usage as in/out parameter
  class Accessor {
   public:
    Accessor();
    Accessor(FileHandleCachePartition::CacheType::Accessor&& cache_accessor);
    Accessor(Accessor&&) = default;
    Accessor& operator=(Accessor&&) = default;

    DISALLOW_COPY_AND_ASSIGN(Accessor);

    /// Handles metrics and unbuffering
    ~Accessor();

    /// Set function can be used if the Accessor is used as in/out parameter.
    void Set(FileHandleCachePartition::CacheType::Accessor&& cache_accessor);

    /// Interface mimics LruMultiCache::Accessor's interface, handles metrics
    CachedHdfsFileHandle* Get();
    void Release();
    void Destroy();

   private:
    FileHandleCachePartition::CacheType::Accessor cache_accessor_;
  };

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

  /// Get a file handle accessor from the cache for the specified filename (fname) and
  /// last modification time (mtime). This will hash the filename to determine
  /// which partition to use for this file handle.
  ///
  /// If 'require_new_handle' is false and the partition contains an available handle,
  /// an accessor is returned and cache_hit is set to true. Otherwise, the partition will
  /// emplace file handle, an accessor to it will be returned with cache_hit set to false.
  /// On failure, empty accessor will be returned. In either case, the partition may evict
  /// a file handle to make room for the new file handle.
  ///
  /// This obtains exclusive control over the returned file handle.
  Status GetFileHandle(const hdfsFS& fs, std::string* fname, int64_t mtime,
      bool require_new_handle, Accessor* accessor, bool* cache_hit) WARN_UNUSED_RESULT;

 private:
  /// Periodic check to evict unused file handles. Only executed by eviction_thread_.
  void EvictHandlesLoop();
  static const int64_t EVICT_HANDLES_PERIOD_MS = 1000;

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
