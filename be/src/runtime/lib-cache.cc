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

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <dlfcn.h>
#include "runtime/hdfs-fs-cache.h"
#include "util/dynamic-util.h"
#include "util/hdfs-util.h"

using namespace boost;
using namespace std;
using namespace impala;

DEFINE_string(local_library_dir, "/tmp",
              "Local directory to copy UDF libraries from HDFS into");

LibCache::~LibCache() {
  BOOST_FOREACH(LibMap::value_type& v, lib_cache_) {
    int error = dlclose(v.second);
    if (error != 0) {
      LOG(WARNING) << "Error calling dlclose for " << v.first
                   << ": (Error: " << error << ") " << dlerror();
    }
  }
}

Status LibCache::GetFunctionPtr(RuntimeState* state, const string& hdfs_lib_file,
                                const string& symbol, void** fn_ptr) {
  lock_guard<mutex> l(lock_);
  SymbolMap::key_type key = make_pair(hdfs_lib_file, symbol);
  if (symbol_cache_[key] == NULL) {
    void* handle;
    RETURN_IF_ERROR(GetHandle(state, hdfs_lib_file, &handle));
    RETURN_IF_ERROR(DynamicLookup(state, handle, symbol.c_str(), &symbol_cache_[key]));
  }
  DCHECK(symbol_cache_[key] != NULL);
  *fn_ptr = symbol_cache_[key];
  return Status::OK;
}

Status LibCache::GetHandle(RuntimeState* state, const string& hdfs_lib_file,
                           void** handle) {
  if (lib_cache_[hdfs_lib_file] == NULL) {
    // Copy the library file from HDFS to the local filesystem
    hdfsFS hdfs_conn = state->fs_cache()->GetDefaultConnection();
    hdfsFS local_conn = state->fs_cache()->GetLocalConnection();
    string local_location;
    RETURN_IF_ERROR(CopyHdfsFile(hdfs_conn, hdfs_lib_file.c_str(), local_conn,
                                 FLAGS_local_library_dir.c_str(), &local_location));

    // dlopen the local library
    RETURN_IF_ERROR(
        DynamicOpen(state, local_location, RTLD_NOW, &lib_cache_[hdfs_lib_file]));
  }
  DCHECK(lib_cache_[hdfs_lib_file] != NULL);
  *handle = lib_cache_[hdfs_lib_file];
  return Status::OK;
}
