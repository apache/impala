// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_RUNTIME_LIB_CACHE_H
#define IMPALA_RUNTIME_LIB_CACHE_H

#include <string>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread/mutex.hpp>
#include "common/object-pool.h"
#include "common/status.h"

namespace impala {

class HdfsFsCache;
class RuntimeState;
class HdfsFsCache;

// Process-wide cache of dynamically-linked libraries loaded from HDFS.
// These libraries can either be shared objects, llvm modules or jars. For
// shared objects, when we load the shared object, we dlopen() it and keep
// it in our process. For modules, we store the symbols in the module to
// service symbol lookups. We can't cache the module since it (i.e. the external
// module) is consumed when it is linked with the query codegen module.
//
// Locking strategy: We don't want to grab a big lock across all operations since
// one of the operations is copying a file from HDFS. With one lock that would
// prevent any UDFs from running on the system. Instead, we have a global lock
// that is taken when doing the cache lookup, but is not taking during any blocking calls.
// During the block calls, we take the per-lib lock.
//
// TODO:
// - refresh libraries
// - better cached module management.
class LibCache {
 public:
  enum LibType {
    TYPE_SO,      // Shared object
    TYPE_IR,      // IR intermediate
    TYPE_JAR,     // Java jar file. We don't care about the contents in the BE.
  };

  LibCache();

  // Calls dlclose on all cached handles.
  ~LibCache();

  // Initializes the libcache. Must be called before any other APIs.
  Status Init(bool is_fe_tests = false);

  // Gets the local file system path for the library at 'hdfs_lib_file'. If
  // this file is not already on the local fs, it copies it and caches the
  // result. Returns an error if 'hdfs_lib_file' cannot be copied to the local fs.
  Status GetLocalLibPath(HdfsFsCache* hdfs_cache, const std::string& hdfs_lib_file,
      LibType type, std::string* local_path);

  // Copies 'hdfs_lib_file' to 'FLAGS_local_library_dir' and dlopens it, storing the
  // result in *handle.
  // Only callable if 'hdfs_lib_file' is a shared object.
  Status GetSoHandle(HdfsFsCache* hdfs_cache,
      const std::string& hdfs_lib_file, void** handle);

  // Returns status.ok() if the symbol exists in 'hdfs_lib_file', non-ok otherwise.
  Status CheckSymbolExists(HdfsFsCache* hdfs_cache, const std::string& hdfs_lib_file,
      LibType type, const std::string& symbol);

  // Returns a pointer to the function for the given library and symbol.
  // If 'hdfs_lib_file' is empty, the symbol is looked up in the impalad process.
  // Otherwise, 'hdfs_lib_file' should be the HDFS path to a shared library (.so) file.
  // dlopen handles and symbols are cached.
  // Only usable if 'hdfs_lib_file' refers to a shared object.
  Status GetSoFunctionPtr(HdfsFsCache* hdfs_cache, const std::string& hdfs_lib_file,
                        const std::string& symbol, void** fn_ptr);

  // Removes the cache entry for 'hdfs_lib_file'
  void RemoveEntry(const std::string hdfs_lib_file);

  // Removes all cached entries.
  void DropCache();

 private:
  // dlopen() handle for the current process (i.e. impalad).
  void* current_process_handle_;

  // Protects lib_cache_. For lock ordering, this lock must always be taken before
  // the per entry lock.
  boost::mutex lock_;

  struct LibCacheEntry {
    // Lock protecting all fields in this entry
    boost::mutex lock;

    // The type of this file.
    LibType type;

    // The path on the local file system for this library.
    std::string local_path;

    // Status returned from copying this file from HDFS.
    Status copy_file_status;

    // Handle from dlopen.
    void* shared_object_handle;

    // mapping from symbol => address of loaded symbol.
    // Only used if the type is TYPE_SO is true.
    typedef boost::unordered_map<std::string, void*> SymbolMap;
    SymbolMap symbol_cache;

    // Set of symbols in this entry. This is populated once on load and read
    // only. This is only used if it is a llvm module.
    // TODO: it would be nice to be able to do this for .so's as well but it's
    // not trivial to walk an .so for the symbol table.
    boost::unordered_set<std::string> symbols;

    LibCacheEntry() : shared_object_handle(NULL) {}
    ~LibCacheEntry();
  };

  // Maps HDFS library path => cache entry.
  // Entries in the cache need to be explicitly deleted.
  typedef boost::unordered_map<std::string, LibCacheEntry*> LibMap;
  LibMap lib_cache_;

  // Returns the cache entry for 'hdfs_lib_file'. If this library has not been
  // copied locally, it will copy it and add a new LibCacheEntry to 'lib_cache_'.
  // Result is returned in *entry.
  // No locks should be take before calling this. On return the entry's lock is
  // taken and returned in *entry_lock.
  Status GetCacheEntry(HdfsFsCache* hdfs_cache, const std::string& hdfs_lib_file,
      LibType type, boost::unique_lock<boost::mutex>* entry_lock,
      LibCacheEntry** entry);

  // Utility function for generating a filename unique to this process and
  // 'hdfs_path'. This is to prevent multiple impalad processes or different library files
  // with the same name from clobbering each other. 'hdfs_path' should be the full path
  // (including the filename) of the file we're going to copy to the local FS, and
  // 'local_dir' is the local directory prefix of the returned path.
  std::string MakeLocalPath(const std::string& hdfs_path, const std::string& local_dir);
};

}

#endif
