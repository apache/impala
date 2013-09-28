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
#include <boost/thread/mutex.hpp>
#include "common/status.h"

namespace impala {

class RuntimeState;

// Process-wide cache of dynamically-linked libraries loaded from HDFS.
//
// TODO:
// - refresh libraries
// - LLVM modules
// - finer-grained locking
class LibCache {
 public:
  // Calls dlclose on all cached handles.
  ~LibCache();

  // Returns a pointer to the function for the given library and symbol. 'hdfs_lib_file'
  // should be the HDFS path to a shared library (.so) file and 'symbol' should be a
  // symbol within that library. dlopen handles and symbols are cached.
  Status GetFunctionPtr(RuntimeState* state, const std::string& hdfs_lib_file,
                        const std::string& symbol, void** fn_ptr);

 private:
  // Protects lib_cache_ and symbol_cache_
  // TODO: This is too coarse. We need to lock per cache entry.
  boost::mutex lock_;

  // Maps HDFS library path => dlopen handle to local library
  typedef boost::unordered_map<std::string, void*> LibMap;
  LibMap lib_cache_;

  // Maps (HDFS library path, symbol) => address of the loaded symbol
  typedef boost::unordered_map<std::pair<std::string, std::string>, void*> SymbolMap;
  SymbolMap symbol_cache_;

  // Copies 'hdfs_lib_file' to 'FLAGS_local_library_dir' and dlopens it, storing the
  // result in *handle. lock_ must be taken before calling this function.
  Status GetHandle(RuntimeState* state, const std::string& hdfs_lib_file, void** handle);
};

}

#endif
