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

#include "runtime/lib-cache.h"

#include <boost/filesystem.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <dlfcn.h>
#include "codegen/llvm-codegen.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-state.h"
#include "util/dynamic-util.h"
#include "util/hash-util.h"
#include "util/hdfs-util.h"
#include "util/path-builder.h"

using namespace boost;
using namespace std;
using namespace impala;

DEFINE_string(local_library_dir, "/tmp",
              "Local directory to copy UDF libraries from HDFS into");

LibCache::LibCache() :current_process_handle_(NULL) {
}

LibCache::~LibCache() {
  DropCache();
  if (current_process_handle_ != NULL) dlclose(current_process_handle_);
}

Status LibCache::Init(bool is_fe_tests) {
  if (is_fe_tests) {
    // In the FE tests, NULL gives the handle to the java process.
    // Explicitly load the fe-support shared object.
    string fe_support_path;
    PathBuilder::GetFullBuildPath("service/libfesupport.so", &fe_support_path);
    RETURN_IF_ERROR(DynamicOpen(fe_support_path.c_str(), &current_process_handle_));
  } else {
    RETURN_IF_ERROR(DynamicOpen(NULL, &current_process_handle_));
  }
  DCHECK(current_process_handle_ != NULL)
      << "We should always be able to get current process handle.";
  return Status::OK;
}

LibCache::LibCacheEntry::~LibCacheEntry() {
  if (shared_object_handle != NULL) {
    int error = dlclose(shared_object_handle);
    if (error != 0) {
      LOG(WARNING) << "Error calling dlclose for " << local_path
                   << ": (Error: " << error << ") " << dlerror();
    }
  }
}

Status LibCache::GetSoFunctionPtr(HdfsFsCache* hdfs_cache, const string& hdfs_lib_file,
                                const string& symbol, void** fn_ptr) {
  if (hdfs_lib_file.empty()) {
    // Just loading a function ptr in the current process. No need to take any locks.
    DCHECK(current_process_handle_ != NULL);
    RETURN_IF_ERROR(DynamicLookup(current_process_handle_, symbol.c_str(), fn_ptr));
    return Status::OK;
  }

  unique_lock<mutex> lock;
  LibCacheEntry* entry = NULL;
  RETURN_IF_ERROR(GetCacheEntry(hdfs_cache, hdfs_lib_file, TYPE_SO, &lock, &entry));
  DCHECK(entry != NULL);
  DCHECK_EQ(entry->type, TYPE_SO);

  LibCacheEntry::SymbolMap::iterator it = entry->symbol_cache.find(symbol);
  if (it != entry->symbol_cache.end()) {
    *fn_ptr = it->second;
  } else {
    RETURN_IF_ERROR(DynamicLookup(entry->shared_object_handle, symbol.c_str(), fn_ptr));
    entry->symbol_cache[symbol] = *fn_ptr;
  }
  DCHECK(*fn_ptr != NULL);
  return Status::OK;
}

Status LibCache::GetLocalLibPath(HdfsFsCache* hdfs_cache, const string& hdfs_lib_file,
      LibType type, string* local_path) {
  unique_lock<mutex> lock;
  LibCacheEntry* entry = NULL;
  RETURN_IF_ERROR(GetCacheEntry(hdfs_cache, hdfs_lib_file, type,
      &lock, &entry));
  DCHECK(entry != NULL);
  DCHECK_EQ(entry->type, type);
  *local_path = entry->local_path;
  return Status::OK;
}

Status LibCache::GetSoHandle(HdfsFsCache* hdfs_cache, const string& hdfs_lib_file,
                           void** handle) {
  unique_lock<mutex> lock;
  LibCacheEntry* entry = NULL;
  RETURN_IF_ERROR(GetCacheEntry(hdfs_cache, hdfs_lib_file, TYPE_SO, &lock, &entry));
  DCHECK(entry != NULL);
  DCHECK_EQ(entry->type, TYPE_SO);
  *handle = entry->shared_object_handle;
  return Status::OK;
}

Status LibCache::CheckSymbolExists(HdfsFsCache* hdfs_cache, const string& hdfs_lib_file,
    LibType type, const string& symbol) {
  if (type == TYPE_SO) {
    void* dummy_ptr = NULL;
    return GetSoFunctionPtr(hdfs_cache, hdfs_lib_file, symbol, &dummy_ptr);
  } else if (type == TYPE_IR) {
    unique_lock<mutex> lock;
    LibCacheEntry* entry = NULL;
    RETURN_IF_ERROR(GetCacheEntry(hdfs_cache, hdfs_lib_file, type, &lock, &entry));
    DCHECK(entry != NULL);
    DCHECK_EQ(entry->type, TYPE_IR);
    if (entry->symbols.find(symbol) == entry->symbols.end()) {
      stringstream ss;
      ss << "Symbol '" << symbol << "' does not exist in module: " << hdfs_lib_file;
      return Status(ss.str());
    }
    return Status::OK;
  } else if (type == TYPE_JAR) {
    unique_lock<mutex> lock;
    LibCacheEntry* dummy_entry = NULL;
    return GetCacheEntry(hdfs_cache, hdfs_lib_file, type, &lock, &dummy_entry);
  } else {
    DCHECK(false);
    return Status("Shouldn't get here.");
  }
}

void LibCache::RemoveEntry(const std::string hdfs_lib_file) {
  unique_lock<mutex> lib_cache_lock(lock_);
  LibMap::iterator it = lib_cache_.find(hdfs_lib_file);
  if (it == lib_cache_.end()) return;
  VLOG(1) << "Removed lib cache entry: " << hdfs_lib_file;
  unique_lock<mutex> entry_lock(it->second->lock);

  // We have both locks now so no other thread can be updating lib_cache_
  // or trying to get the entry.

  // Get the entry before removing the iterator.
  LibCacheEntry* entry = it->second;
  lib_cache_.erase(it);

  // Now that the entry is removed from the map, it means no future threads
  // can find it->second (the entry), so it is safe to unlock.
  entry_lock.unlock();

  // Now that we've unlocked, we can delete this entry.
  delete entry;
}

void LibCache::DropCache() {
  unique_lock<mutex> lib_cache_lock(lock_);
  BOOST_FOREACH(LibMap::value_type& v, lib_cache_) {
    {
      // Lock to wait for any threads currently processing the entry.
      unique_lock<mutex> entry_lock(v.second->lock);
    }
    VLOG(1) << "Removed lib cache entry: " << v.first;
    delete v.second;
  }
  lib_cache_.clear();
}

Status LibCache::GetCacheEntry(HdfsFsCache* hdfs_cache, const string& hdfs_lib_file,
    LibType type, unique_lock<mutex>* entry_lock, LibCacheEntry** entry) {
  unique_lock<mutex> lib_cache_lock(lock_);
  LibMap::iterator it = lib_cache_.find(hdfs_lib_file);
  if (it != lib_cache_.end()) {
    *entry = it->second;
    // Release the lib_cache_ lock. This guarantees other threads looking at other
    // libs can continue.
    lib_cache_lock.unlock();
    unique_lock<mutex> local_entry_lock((*entry)->lock);
    entry_lock->swap(local_entry_lock);

    RETURN_IF_ERROR((*entry)->copy_file_status);
    DCHECK_EQ((*entry)->type, type);
    DCHECK(!(*entry)->local_path.empty());
    return Status::OK;
  }
  // Entry didn't exist. Add the entry then release lock_ (so other libraries
  // can be accessed).
  *entry = new LibCacheEntry();

  // Grab the entry lock before adding it to lib_cache_. We still need to do more
  // work to initialize *entry and we don't want another thread to pick up
  // the uninitialized entry.
  unique_lock<mutex> local_entry_lock((*entry)->lock);
  entry_lock->swap(local_entry_lock);
  lib_cache_[hdfs_lib_file] = *entry;
  lib_cache_lock.unlock();

  VLOG(1) << "Added lib cache entry: " << hdfs_lib_file;

  // At this point we have the entry lock but not the lib cache lock.
  DCHECK(*entry != NULL);
  (*entry)->type = type;

  // Copy the file
  (*entry)->local_path = MakeLocalPath(hdfs_lib_file, FLAGS_local_library_dir);
  hdfsFS hdfs_conn = hdfs_cache->GetDefaultConnection();
  hdfsFS local_conn = hdfs_cache->GetLocalConnection();
  (*entry)->copy_file_status = CopyHdfsFile(
      hdfs_conn, hdfs_lib_file, local_conn, (*entry)->local_path);
  RETURN_IF_ERROR((*entry)->copy_file_status);
  if (type == TYPE_SO) {
    // dlopen the local library
    RETURN_IF_ERROR(
        DynamicOpen((*entry)->local_path.c_str(), &(*entry)->shared_object_handle));
  } else if (type == TYPE_IR) {
    // Load the module and populate all symbols.
    ObjectPool pool;
    scoped_ptr<LlvmCodeGen> codegen;
    RETURN_IF_ERROR(LlvmCodeGen::LoadFromFile(&pool, (*entry)->local_path, &codegen));
    codegen->GetSymbols(&(*entry)->symbols);
  } else {
    DCHECK_EQ(type, TYPE_JAR);
    // Nothing to do.
  }

  return Status::OK;
}

string LibCache::MakeLocalPath(const string& hdfs_path, const string& local_dir) {
  stringstream hash_ss;
  hash_ss << getpid() << hdfs_path;
  // Shorten the hash to 16 bits so the filenames aren't too long.
  uint32_t hash = HashUtil::Hash(hash_ss.str().c_str(), hash_ss.str().size(), 0) >> 16;

  // Append the filename + hash to the local directory.
  filesystem::path src(hdfs_path);
  stringstream dst;
  dst << local_dir << "/" << src.stem().native() << "." << hash
      << src.extension().native();
  return dst.str();
}
