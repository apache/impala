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

#include <mutex>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include "common/atomic.h"
#include "common/object-pool.h"
#include "common/status.h"

namespace impala {

class RuntimeState;

/// Process-wide cache of dynamically-linked libraries loaded from HDFS.
/// These libraries can either be shared objects, llvm modules or jars. For
/// shared objects, when we load the shared object, we dlopen() it and keep
/// it in our process. For modules, we store the symbols in the module to
/// service symbol lookups. We can't cache the module since it (i.e. the external
/// module) is consumed when it is linked with the query codegen module.
//
/// Locking strategy: We don't want to grab a big lock across all operations since
/// one of the operations is copying a file from HDFS. With one lock that would
/// prevent any UDFs from running on the system. Instead, we have a global lock
/// that is taken when doing the cache lookup, but is not taken during any blocking calls.
/// During the block calls, we take the per-lib lock.
//
/// Entry lifetime management: We cannot delete the entry while a query is
/// using the library. When the caller requests a ptr into the library, they
/// are given the entry handle and must decrement the ref count when they
/// are done.
/// Note: Explicitly managing this reference count at the client is error-prone. See the
/// api for accessing a path, GetLocalPath(), that uses the handle's scope to manage the
/// reference count.
//
/// TODO:
/// - refresh libraries
/// - better cached module management
/// - improve the api to be less error-prone (IMPALA-6439)
struct LibCacheEntry;
class LibCacheEntryHandle;

class LibCache {
 public:
  enum LibType {
    TYPE_SO,      // Shared object
    TYPE_IR,      // IR intermediate
    TYPE_JAR,     // Java jar file. We don't care about the contents in the BE.
  };

  static LibCache* instance() { return LibCache::instance_.get(); }

  /// Calls dlclose on all cached handles.
  ~LibCache();

  /// Initializes the libcache. Must be called before any other APIs.
  static Status Init(bool external_fe);

  /// Gets the local 'path' used to cache the file stored at the global 'hdfs_lib_file'. If
  /// this file is not already on the local fs, or if the cached entry's last modified
  /// is older than expected mtime, 'exp_mtime', it copies it and caches the result.
  /// An 'exp_mtime' of -1 makes the mtime check a no-op.
  ///
  /// 'handle' must remain in scope while 'path' is used. The reference count to the
  /// underlying cache entry is decremented when 'handle' goes out-of-scope.
  ///
  /// Returns an error if 'hdfs_lib_file' cannot be copied to the local fs or if
  /// exp_mtime differs from the mtime on the file system.
  /// If error is due to refresh, then the entry will be removed from the cache.
  Status GetLocalPath(const std::string& hdfs_lib_file, LibType type, time_t exp_mtime,
      LibCacheEntryHandle* handle, string* path);

  /// Returns status.ok() if the symbol exists in 'hdfs_lib_file', non-ok otherwise.
  /// If status.ok() is true, 'mtime' is set to the cache entry's last modified time.
  /// If an mtime is not applicable, for example, if lookup is for a builtin, then
  /// a default mtime of -1 is set.
  /// If 'quiet' is true, the error status for non-Java unfound symbols will not be
  /// logged.
  Status CheckSymbolExists(const std::string& hdfs_lib_file, LibType type,
      const std::string& symbol, bool quiet, time_t* mtime);

  /// Returns a pointer to the function for the given library and symbol.
  /// If 'hdfs_lib_file' is empty, the symbol is looked up in the impalad process.
  /// Otherwise, 'hdfs_lib_file' should be the HDFS path to a shared library (.so) file.
  /// dlopen handles and symbols are cached.
  /// Only usable if 'hdfs_lib_file' refers to a shared object.
  //
  /// If entry is non-null and *entry is null, *entry will be set to the cached entry. If
  /// entry is non-null and *entry is non-null, *entry will be reused (i.e., the use count
  /// is not increased). The caller must call DecrementUseCount(*entry) when it is done
  /// using fn_ptr and it is no longer valid to use fn_ptr.
  //
  /// If 'quiet' is true, returned error statuses will not be logged.
  /// If the entry is already cached, if its last modified time is older than
  /// expected mtime, 'exp_mtime', the entry is refreshed.
  /// An 'exp_mtime' of -1 makes the mtime check a no-op.
  /// An error is returned if exp_mtime differs from the mtime on the file system.
  /// If error is due to refresh, then the entry will be removed from the cache.
  /// TODO: api is error-prone. upgrade to LibCacheEntryHandle (see IMPALA-6439).
  Status GetSoFunctionPtr(const std::string& hdfs_lib_file, const std::string& symbol,
      time_t exp_mtime, void** fn_ptr, LibCacheEntry** entry, bool quiet = false);

  /// Marks the entry for 'hdfs_lib_file' as needing to be refreshed if the file in HDFS is
  /// newer than the local cached copied. The refresh will occur the next time the entry is
  /// accessed.
  void SetNeedsRefresh(const std::string& hdfs_lib_file);

  /// See comment in GetSoFunctionPtr().
  void DecrementUseCount(LibCacheEntry* entry);

  /// Removes the cache entry for 'hdfs_lib_file'
  void RemoveEntry(const std::string& hdfs_lib_file);

  /// Removes all cached entries.
  void DropCache();

 private:
  /// Singleton instance. Instantiated in Init().
  static boost::scoped_ptr<LibCache> instance_;

  /// dlopen() handle for the current process (i.e. impalad).
  void* current_process_handle_;

  /// The number of libs that have been copied from HDFS to the local FS.
  /// This is appended to the local fs path to remove collisions.
  AtomicInt64 num_libs_copied_;

  /// Protects lib_cache_. For lock ordering, this lock must always be taken before
  /// the per entry lock.
  std::mutex lock_;

  /// Maps HDFS library path => cache entry.
  /// Entries in the cache need to be explicitly deleted.
  typedef boost::unordered_map<std::string, LibCacheEntry*> LibMap;
  LibMap lib_cache_;

  LibCache();
  LibCache(LibCache const& l); // disable copy ctor
  LibCache& operator=(LibCache const& l); // disable assignment

  Status InitInternal(bool external_fe);

  /// Returns the cache entry for 'hdfs_lib_file'. If this library has not been
  /// copied locally, it will copy it and add a new LibCacheEntry to 'lib_cache_'.
  /// If the entry is already cached, if its last modified time is older than
  /// expected mtime, 'exp_mtime', the entry is refreshed. Result is returned in *entry.
  /// An 'exp_mtime' of -1 makes the mtime check a no-op.
  /// An error is returned if exp_mtime differs from the mtime on the file system.
  /// No locks should be taken before calling this. On return the entry's lock is
  /// taken and returned in *entry_lock.
  /// If an error is returned, there will be no entry in lib_cache_ and *entry is NULL.
  Status GetCacheEntry(const std::string& hdfs_lib_file, LibType type, time_t exp_mtime,
      std::unique_lock<std::mutex>* entry_lock, LibCacheEntry** entry);

  /// Implementation to get the cache entry for 'hdfs_lib_file'. Errors are returned
  /// without evicting the cache entry if the status is not OK and *entry is not NULL.
  Status GetCacheEntryInternal(const std::string& hdfs_lib_file, LibType type,
      time_t exp_mtime, std::unique_lock<std::mutex>* entry_lock, LibCacheEntry** entry);

  /// Returns iter's cache entry in 'entry' with 'entry_lock' held if entry does not
  /// need to be refreshed.
  /// If entry needs to be refreshed, then it is removed and '*entry' is set to nullptr.
  /// The entry is refreshed if needs_refresh is set and its mtime is
  /// older than the file on the fs OR its mtime is older than the
  /// 'exp_mtime' argument.
  /// An 'exp_mtime' of -1 makes the mtime check a no-op.
  /// An error is returned if exp_mtime differs from the mtime on the file system.
  /// If an error occurs when refreshing the entry, the entry is removed.
  /// The cache lock must be held prior to calling this method. On return the entry's
  /// lock is taken and returned in '*entry_lock' if entry does not need to be refreshed.
  /// TODO: cleanup this method's interface and how the outputs are used.
  Status RefreshCacheEntry(const std::string& hdfs_lib_file, LibType type,
      time_t exp_mtime, const LibMap::iterator& iter,
      std::unique_lock<std::mutex>* entry_lock, LibCacheEntry** entry);

  /// 'hdfs_lib_file' is copied locally and 'entry' is initialized with its contents.
  /// An error is returned if exp_mtime differs from the mtime on the file system.
  /// An 'exp_mtime' of -1 makes the mtime check a no-op.
  /// No locks are assumed held; 'entry' should be visible only to a single thread.
  Status LoadCacheEntry(const std::string& hdfs_lib_file, time_t exp_mtime, LibType type,
      LibCacheEntry* entry);

  /// Utility function for generating a filename unique to this process and
  /// 'hdfs_path'. This is to prevent multiple impalad processes or different library files
  /// with the same name from clobbering each other. 'hdfs_path' should be the full path
  /// (including the filename) of the file we're going to copy to the local FS, and
  /// 'local_dir' is the local directory prefix of the returned path.
  std::string MakeLocalPath(const std::string& hdfs_path, const std::string& local_dir);

  /// Implementation to remove an entry from the cache.
  /// lock_ must be held. The entry's lock should not be held.
  void RemoveEntryInternal(
      const std::string& hdfs_lib_file, const LibMap::iterator& entry_iterator);
};

/// Handle for a LibCacheEntry that decrements its reference count when the handle is
/// destroyed or re-used for another entry.
class LibCacheEntryHandle {
 public:
  LibCacheEntryHandle() {}
  ~LibCacheEntryHandle();

 private:
  friend class LibCache;

  LibCacheEntry* entry() const { return entry_; }
  void SetEntry(LibCacheEntry* entry) {
    if (entry_ != nullptr) LibCache::instance()->DecrementUseCount(entry);
    entry_ = entry;
  }

  LibCacheEntry* entry_ = nullptr;

  DISALLOW_COPY_AND_ASSIGN(LibCacheEntryHandle);
};

}
