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
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>
#include <hdfs.h>

namespace impala {

// A (process-wide) cache of hdfsFS objects.
// These connections are shared across all threads and kept
// open until the process terminates.
// (Calls to hdfsDisconnect() by individual threads would terminate all
// other connections handed out via hdfsConnect() to the same URI.)
class HdfsFsCache {
 public:
  ~HdfsFsCache();

  // Get connection to default fs.
  hdfsFS GetDefaultConnection();

  // Get connection to specific fs by specifying the name node's
  // ipaddress or hostname and port.
  hdfsFS GetConnection(const std::string& host, int port);

 private:
  boost::mutex lock_;  // protects fs_map_
  typedef boost::unordered_map<std::pair<std::string, int>, hdfsFS> HdfsFsMap;
  HdfsFsMap fs_map_;
};

}

#endif
