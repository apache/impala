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


#ifndef IMPALA_RUNTIME_HDFS_FS_CACHE_H
#define IMPALA_RUNTIME_HDFS_FS_CACHE_H

#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>
#include <hdfs.h>

#include "common/status.h"

namespace impala {

// A (process-wide) cache of hdfsFS objects.
// These connections are shared across all threads and kept open until the process
// terminates.
//
// These connections are leaked, i.e. we never call hdfsDisconnect(). Calls to
// hdfsDisconnect() by individual threads would terminate all other connections handed
// out via hdfsConnect() to the same URI, and there is no simple, safe way to call
// hdfsDisconnect() when process terminates (the proper solution is likely to create a
// signal handler to detect when the process is killed, but we would still leak when
// impalad crashes).
class HdfsFsCache {
 public:
  static HdfsFsCache* instance() { return HdfsFsCache::instance_.get(); }

  // Initializes the cache. Must be called before any other APIs.
  static void Init();

  // Get connection to the local filesystem.
  Status GetLocalConnection(hdfsFS* fs);

  // Get connection to specific fs by specifying a path.
  Status GetConnection(const std::string& path, hdfsFS* fs);

 private:
  // Singleton instance. Instantiated in Init().
  static boost::scoped_ptr<HdfsFsCache> instance_;

  boost::mutex lock_;  // protects fs_map_
  typedef boost::unordered_map<std::string, hdfsFS> HdfsFsMap;
  HdfsFsMap fs_map_;

  HdfsFsCache() { };
  HdfsFsCache(HdfsFsCache const& l); // disable copy ctor
  HdfsFsCache& operator=(HdfsFsCache const& l); // disable assignment
};

}

#endif
