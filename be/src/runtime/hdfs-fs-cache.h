// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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
  // host and port.
  hdfsFS GetConnection(const std::string& host, int port);

 private:
  boost::mutex lock_;  // protects fs_map_
  typedef boost::unordered_map<std::pair<std::string, int>, hdfsFS> HdfsFsMap;
  HdfsFsMap fs_map_;
};

}

#endif
