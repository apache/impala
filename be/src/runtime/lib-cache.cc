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

#include "runtime/lib-cache.h"

#include <mutex>

#include <boost/filesystem.hpp>

#include "codegen/llvm-codegen.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-state.h"
#include "util/dynamic-util.h"
#include "util/hash-util.h"
#include "util/hdfs-util.h"
#include "util/path-builder.h"
#include "util/test-info.h"

#include "common/names.h"

namespace filesystem = boost::filesystem;

DECLARE_string(local_library_dir);

namespace impala {

scoped_ptr<LibCache> LibCache::instance_;

struct LibCacheEntry {
  // Lock protecting all fields in this entry
  std::mutex lock;

  // The number of users that are using this cache entry. If this is
  // a .so, we can't dlclose unless the use_count goes to 0.
  int use_count;

  // If true, this cache entry should be removed from lib_cache_ when
  // the use_count goes to 0.
  bool should_remove;

  // If true, we need to check if there is a newer version of the cached library in HDFS
  // on next access. Should hold lock_ to read/write.
  bool check_needs_refresh;

  // The type of this file.
  LibCache::LibType type;

  // The path on the local file system for this library.
  std::string local_path;

  // Status returned from copying this file from HDFS.
  Status copy_file_status;

  // The last modification time of the HDFS file in seconds.
  time_t last_mod_time;

  // Handle from dlopen.
  void* shared_object_handle;

  // mapping from symbol => address of loaded symbol.
  // Only used if the type is TYPE_SO.
  typedef boost::unordered_map<std::string, void*> SymbolMap;
  SymbolMap symbol_cache;

  // Set of symbols in this entry. This is populated once on load and read
  // only. This is only used if it is a llvm module.
  // TODO: it would be nice to be able to do this for .so's as well but it's
  // not trivial to walk an .so for the symbol table.
  boost::unordered_set<std::string> symbols;

  // Set if an error occurs loading the cache entry before the cache entry
  // can be evicted. This allows other threads that attempt to use the entry
  // before it is removed to return the same error.
  Status loading_status;

  LibCacheEntry()
    : use_count(0),
      should_remove(false),
      check_needs_refresh(false),
      shared_object_handle(nullptr) {}
  ~LibCacheEntry();
};

LibCache::LibCache() : current_process_handle_(nullptr) {}

LibCache::~LibCache() {
  DropCache();
  if (current_process_handle_ != nullptr) DynamicClose(current_process_handle_);
}

Status LibCache::Init(bool external_fe) {
  DCHECK(LibCache::instance_.get() == nullptr);
  LibCache::instance_.reset(new LibCache());
  return LibCache::instance_->InitInternal(external_fe);
}

Status LibCache::InitInternal(bool external_fe) {
  if (external_fe) {
    LOG(INFO) << "Library cache is using shared object for process handle";
    RETURN_IF_ERROR(DynamicOpen("libfesupport.so", &current_process_handle_));
  } else if (TestInfo::is_fe_test()) {
    // In the FE tests, nullptr gives the handle to the java process.
    // Explicitly load the fe-support shared object.
    string fe_support_path;
    PathBuilder::GetFullBuildPath("service/libfesupport.so", &fe_support_path);
    RETURN_IF_ERROR(DynamicOpen(fe_support_path.c_str(), &current_process_handle_));
  } else {
    RETURN_IF_ERROR(DynamicOpen(nullptr, &current_process_handle_));
  }
  DCHECK(current_process_handle_ != nullptr)
      << "We should always be able to get current process handle.";
  return Status::OK();
}

LibCacheEntry::~LibCacheEntry() {
  if (shared_object_handle != nullptr) {
    DCHECK_EQ(use_count, 0);
    DCHECK(should_remove);
    DynamicClose(shared_object_handle);
  }
  unlink(local_path.c_str());
}

LibCacheEntryHandle::~LibCacheEntryHandle() {
  if (entry_ != nullptr) LibCache::instance()->DecrementUseCount(entry_);
}

Status LibCache::GetSoFunctionPtr(const string& hdfs_lib_file, const string& symbol,
    time_t exp_mtime, void** fn_ptr, LibCacheEntry** ent, bool quiet) {
  if (hdfs_lib_file.empty()) {
    // Just loading a function ptr in the current process. No need to take any locks.
    DCHECK(current_process_handle_ != nullptr);
    RETURN_IF_ERROR(DynamicLookup(current_process_handle_, symbol.c_str(), fn_ptr, quiet));
    return Status::OK();
  }

  LibCacheEntry* entry = nullptr;
  unique_lock<mutex> lock;
  if (ent != nullptr && *ent != nullptr) {
    // Reuse already-cached entry provided by user
    entry = *ent;
    unique_lock<mutex> l(entry->lock);
    lock.swap(l);
  } else {
    RETURN_IF_ERROR(GetCacheEntry(hdfs_lib_file, TYPE_SO, exp_mtime, &lock, &entry));
  }
  DCHECK(entry != nullptr);
  DCHECK_EQ(entry->type, TYPE_SO);
  LibCacheEntry::SymbolMap::iterator it = entry->symbol_cache.find(symbol);
  if (it != entry->symbol_cache.end()) {
    *fn_ptr = it->second;
  } else {
    RETURN_IF_ERROR(
        DynamicLookup(entry->shared_object_handle, symbol.c_str(), fn_ptr, quiet));
    entry->symbol_cache[symbol] = *fn_ptr;
  }

  DCHECK(*fn_ptr != nullptr);
  if (ent != nullptr && *ent == nullptr) {
    // Only set and increment user's entry if it wasn't already cached
    *ent = entry;
    ++(*ent)->use_count;
  }
  return Status::OK();
}

void LibCache::DecrementUseCount(LibCacheEntry* entry) {
  if (entry == nullptr) return;
  bool can_delete = false;
  {
    unique_lock<mutex> lock(entry->lock);
    --entry->use_count;
    can_delete = (entry->use_count == 0 && entry->should_remove);
  }
  if (can_delete) delete entry;
}

Status LibCache::GetLocalPath(const std::string& hdfs_lib_file, LibType type,
    time_t exp_mtime, LibCacheEntryHandle* handle, string* path) {
  DCHECK(handle != nullptr && handle->entry() == nullptr);
  LibCacheEntry* entry = nullptr;
  unique_lock<mutex> lock;
  RETURN_IF_ERROR(GetCacheEntry(hdfs_lib_file, type, exp_mtime, &lock, &entry));
  DCHECK(entry != nullptr);
  ++entry->use_count;
  handle->SetEntry(entry);
  *path = entry->local_path;
  return Status::OK();
}

Status LibCache::CheckSymbolExists(const string& hdfs_lib_file, LibType type,
    const string& symbol, bool quiet, time_t* mtime) {
  if (type == TYPE_SO) {
    void* dummy_ptr = nullptr;
    LibCacheEntry* entry = nullptr;
    RETURN_IF_ERROR(
        GetSoFunctionPtr(hdfs_lib_file, symbol, -1, &dummy_ptr, &entry, quiet));
    *mtime = -1;
    if (entry != nullptr) {
      *mtime = entry->last_mod_time;
      // done holding this entry, so decrement its use count.
      DecrementUseCount(entry);
    }
    return Status::OK();
  } else if (type == TYPE_IR) {
    unique_lock<mutex> lock;
    LibCacheEntry* entry = nullptr;
    RETURN_IF_ERROR(GetCacheEntry(hdfs_lib_file, type, -1, &lock, &entry));
    DCHECK(entry != nullptr);
    DCHECK_EQ(entry->type, TYPE_IR);
    if (entry->symbols.find(symbol) == entry->symbols.end()) {
      stringstream ss;
      ss << "Symbol '" << symbol << "' does not exist in module: " << hdfs_lib_file
         << " (local path: " << entry->local_path << ")";
      return quiet ? Status::Expected(ss.str()) : Status(ss.str());
    }
    *mtime = entry->last_mod_time;
    return Status::OK();
  } else if (type == TYPE_JAR) {
    // TODO: figure out how to inspect contents of jars
    unique_lock<mutex> lock;
    LibCacheEntry* entry = nullptr;
    RETURN_IF_ERROR(GetCacheEntry(hdfs_lib_file, type, -1, &lock, &entry));
    *mtime = entry->last_mod_time;
    return Status::OK();
  } else {
    DCHECK(false);
    return Status("Shouldn't get here.");
  }
}

void LibCache::SetNeedsRefresh(const string& hdfs_lib_file) {
  unique_lock<mutex> lib_cache_lock(lock_);
  LibMap::iterator it = lib_cache_.find(hdfs_lib_file);
  if (it == lib_cache_.end()) return;
  LibCacheEntry* entry = it->second;

  unique_lock<mutex> entry_lock(entry->lock);
  // Need to hold lock_ before setting check_needs_refresh.
  entry->check_needs_refresh = true;
}

void LibCache::RemoveEntry(const string& hdfs_lib_file) {
  unique_lock<mutex> lib_cache_lock(lock_);
  LibMap::iterator it = lib_cache_.find(hdfs_lib_file);
  if (it == lib_cache_.end()) return;
  RemoveEntryInternal(hdfs_lib_file, it);
}

void LibCache::RemoveEntryInternal(
    const string& hdfs_lib_file, const LibMap::iterator& entry_iter) {
  LibCacheEntry* entry = entry_iter->second;
  VLOG(1) << "Removing lib cache entry: " << hdfs_lib_file
          << ", local path: " << entry->local_path;
  unique_lock<mutex> entry_lock(entry->lock);

  // We have both locks so no other thread can be updating lib_cache_ or trying to get
  // the entry.
  lib_cache_.erase(entry_iter);

  entry->should_remove = true;
  DCHECK_GE(entry->use_count, 0);
  bool can_delete = entry->use_count == 0;

  // Now that the entry is removed from the map, it means no future threads
  // can find it->second (the entry), so it is safe to unlock.
  entry_lock.unlock();

  // Now that we've unlocked, we can delete this entry if no one is using it.
  if (can_delete) delete entry;
}

void LibCache::DropCache() {
  unique_lock<mutex> lib_cache_lock(lock_);
  for (LibMap::value_type& v: lib_cache_) {
    bool can_delete = false;
    {
      // Lock to wait for any threads currently processing the entry.
      unique_lock<mutex> entry_lock(v.second->lock);
      v.second->should_remove = true;
      DCHECK_GE(v.second->use_count, 0);
      can_delete = v.second->use_count == 0;
    }
    VLOG(1) << "Removed lib cache entry: " << v.first;
    if (can_delete) delete v.second;
  }
  lib_cache_.clear();
}

Status LibCache::GetCacheEntry(const string& hdfs_lib_file, LibType type,
    time_t exp_mtime, unique_lock<mutex>* entry_lock, LibCacheEntry** entry) {
  Status status;
  {
    // If an error occurs, local_entry_lock is released before calling RemoveEntry()
    // below because it takes the global lock_ which must be acquired before taking entry
    // locks.
    unique_lock<mutex> local_entry_lock;
    status =
        GetCacheEntryInternal(hdfs_lib_file, type, exp_mtime, &local_entry_lock, entry);
    if (status.ok()) {
      entry_lock->swap(local_entry_lock);
      return status;
    }
    if (*entry == nullptr) return status;

    // Set loading_status on the entry so that if another thread calls
    // GetCacheEntry() for this lib before this thread is able to acquire lock_ in
    // RemoveEntry(), it is able to return the same error.
    (*entry)->loading_status = status;
  }
  // Takes lock_
  RemoveEntry(hdfs_lib_file);
  return status;
}

Status LibCache::GetCacheEntryInternal(const string& hdfs_lib_file, LibType type,
    time_t exp_mtime, unique_lock<mutex>* entry_lock, LibCacheEntry** entry) {
  DCHECK(!hdfs_lib_file.empty());
  *entry = nullptr;

  // Check if this file is already cached. Refresh the entry if needed.
  {
    unique_lock<mutex> lib_cache_lock(lock_);
    LibMap::iterator it = lib_cache_.find(hdfs_lib_file);
    if (it != lib_cache_.end()) {
      RETURN_IF_ERROR(
          RefreshCacheEntry(hdfs_lib_file, type, exp_mtime, it, entry_lock, entry));
      if (*entry != nullptr) return Status::OK();
    }
  }

  // Entry didn't exist. Create a new entry and load it. Note that the cache lock is
  // *not* held and the entry is not added to the cache until it is loaded. Loading is
  // expensive, so *not* holding the cache lock and *not* making the entry visible to
  // other threads avoids blocking other threads with an expensive operation.
  unique_ptr<LibCacheEntry> new_entry = make_unique<LibCacheEntry>();
  RETURN_IF_ERROR(LoadCacheEntry(hdfs_lib_file, exp_mtime, type, new_entry.get()));

  // Entry is now loaded. Check that another thread did not already load and add an entry
  // for the same key. If so, refresh it if needed. If the existing entry is valid, then
  // use it and discard new_entry.
  {
    unique_lock<mutex> lib_cache_lock(lock_);
    LibMap::iterator it = lib_cache_.find(hdfs_lib_file);
    if (it != lib_cache_.end()) {
      Status status =
          RefreshCacheEntry(hdfs_lib_file, type, exp_mtime, it, entry_lock, entry);
      // The entry lock is held at this point if entry is valid.
      if (!status.ok() || *entry != nullptr) {
        // new_entry will be discarded; while wasted work, it avoids holding
        // the cache lock while loading.
        new_entry->should_remove = true;
        return status;
      }
    }

    // The entry was not found or was removed for refresh. Use the new entry, so
    // lock it and add it to the cache.
    *entry = new_entry.release();
    unique_lock<mutex> local_entry_lock((*entry)->lock);
    entry_lock->swap(local_entry_lock);
    lib_cache_[hdfs_lib_file] = *entry;
  }
  return Status::OK();
}

Status LibCache::RefreshCacheEntry(const string& hdfs_lib_file, LibType type,
    time_t exp_mtime, const LibMap::iterator& iter, unique_lock<mutex>* entry_lock,
    LibCacheEntry** entry) {
  // Check if an error occurred on another thread while loading the library.
  {
    unique_lock<mutex> local_entry_lock((iter->second)->lock);
    if (!(iter->second)->loading_status.ok()) {
      // If loading_status is already set, the returned *entry should be nullptr.
      DCHECK(*entry == nullptr);
      return (iter->second)->loading_status;
    }
  }

  // Refresh the cache entry if needed. A refresh is needed if check_needs_refresh is set
  // (e.g., set by ddl) or if the exp_mtime argument is more recent.
  // If refreshed or an error occurred, remove the entry and set the returned entry to
  // nullptr.
  *entry = iter->second;
  if ((*entry)->check_needs_refresh || (*entry)->last_mod_time < exp_mtime) {
    // Check if file has been modified since loading the cached copy. If so, remove the
    // cached entry and create a new one.
    (*entry)->check_needs_refresh = false;
    hdfsFS hdfs_conn;
    Status status = HdfsFsCache::instance()->GetConnection(hdfs_lib_file, &hdfs_conn);
    if (!status.ok()) {
      RemoveEntryInternal(hdfs_lib_file, iter);
      *entry = nullptr;
      return status;
    }
    time_t fs_last_modified_time;
    status =
        GetLastModificationTime(hdfs_conn, hdfs_lib_file.c_str(), &fs_last_modified_time);

    // Check that the expected last_modified_time is the same as what's on the filesystem.
    if (status.ok() && exp_mtime >= 0 && fs_last_modified_time != exp_mtime) {
      status = Status(TErrorCode::LIB_VERSION_MISMATCH, hdfs_lib_file,
          fs_last_modified_time, exp_mtime);
    }
    if (!status.ok() || (*entry)->last_mod_time < fs_last_modified_time) {
      RemoveEntryInternal(hdfs_lib_file, iter);
      *entry = nullptr;
    }
    RETURN_IF_ERROR(status);
  }

  // No refresh needed, the entry can be used.
  if (*entry != nullptr) {
    // The cache level lock continues to be held while the entry lock is obtained
    // so that some other thread does not access the entry and delete it.
    unique_lock<mutex> local_entry_lock((*entry)->lock);
    entry_lock->swap(local_entry_lock);

    // Let the caller propagate any error that occurred when loading the entry.
    RETURN_IF_ERROR((*entry)->copy_file_status);
    DCHECK_EQ((*entry)->type, type) << (*entry)->local_path;
    DCHECK(!(*entry)->local_path.empty());
  }
  return Status::OK();
}

Status LibCache::LoadCacheEntry(const std::string& hdfs_lib_file, time_t exp_mtime,
    LibType type, LibCacheEntry* entry) {
  DCHECK(entry != nullptr);
  entry->type = type;

  // Copy the file
  entry->local_path = MakeLocalPath(hdfs_lib_file, FLAGS_local_library_dir);
  VLOG(1) << "Adding lib cache entry: " << hdfs_lib_file
          << ", local path: " << entry->local_path;

  hdfsFS hdfs_conn, local_conn;
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(hdfs_lib_file, &hdfs_conn));
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetLocalConnection(&local_conn));

  // Note: the file can be updated between getting last_mod_time and copying the file to
  // local_path. This can only result in the file unnecessarily being refreshed, and does
  // not affect correctness.
  entry->copy_file_status =
      GetLastModificationTime(hdfs_conn, hdfs_lib_file.c_str(), &entry->last_mod_time);
  RETURN_IF_ERROR(entry->copy_file_status);

  // Check that the exp_mtime is the same as what's on the filesystem.
  if (exp_mtime >= 0 && exp_mtime != entry->last_mod_time) {
    return Status(
        TErrorCode::LIB_VERSION_MISMATCH, hdfs_lib_file, entry->last_mod_time, exp_mtime);
  }

  entry->copy_file_status =
      CopyHdfsFile(hdfs_conn, hdfs_lib_file, local_conn, entry->local_path);
  RETURN_IF_ERROR(entry->copy_file_status);

  if (type == TYPE_SO) {
    // dlopen the local library
    RETURN_IF_ERROR(DynamicOpen(entry->local_path.c_str(), &entry->shared_object_handle));
  } else if (type == TYPE_IR) {
    // Load the module temporarily and populate all symbols.
    const string file = entry->local_path;
    const string module_id = filesystem::path(file).stem().string();
    RETURN_IF_ERROR(LlvmCodeGen::GetSymbols(file, module_id, &entry->symbols));
  } else {
    DCHECK_EQ(type, TYPE_JAR);
    // Nothing to do.
  }
  return Status::OK();
}

string LibCache::MakeLocalPath(const string& hdfs_path, const string& local_dir) {
  // Append the pid and library number to the local directory.
  filesystem::path src(hdfs_path);
  stringstream dst;
  dst << local_dir << "/" << src.stem().native() << "." << getpid() << "."
      << (num_libs_copied_.Add(1) - 1) << src.extension().native();
  return dst.str();
}

}
