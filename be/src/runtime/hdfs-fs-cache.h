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
#include "common/hdfs.h"

#include "common/status.h"

namespace impala {

/// A (process-wide) cache of hdfsFS objects.
/// These connections are shared across all threads and kept open until the process
/// terminates.
//
/// These connections are leaked, i.e. we never call hdfsDisconnect(). Calls to
/// hdfsDisconnect() by individual threads would terminate all other connections handed
/// out via hdfsConnect() to the same URI, and there is no simple, safe way to call
/// hdfsDisconnect() when process terminates (the proper solution is likely to create a
/// signal handler to detect when the process is killed, but we would still leak when
/// impalad crashes).
class HdfsFsCache {
 public:
  typedef boost::unordered_map<std::string, hdfsFS> HdfsFsMap;
  typedef std::vector<std::pair<string, string>> HdfsConnOptions;

  static HdfsFsCache* instance() { return HdfsFsCache::instance_.get(); }

  /// Initializes the cache. Must be called before any other APIs.
  static Status Init();

  /// Get connection to the local filesystem.
  Status GetLocalConnection(hdfsFS* fs);

  /// Get connection to specific fs by specifying a path.  Optionally, a local cache can
  /// be provided so that the process-wide lock can be avoided on subsequent calls for
  /// the same filesystem. Also, options can be provided to create the hdfs connection.
  /// The caller is responsible for synchronizing the local cache
  /// (e.g. by passing a thread-local cache).
  Status GetConnection(const std::string& path, hdfsFS* fs, HdfsFsMap* local_cache = NULL,
      const HdfsConnOptions* options = NULL);

  /// Get NameNode info from path, set error message if path is not valid.
  /// Exposed as a static method for testing purpose.
  static std::string GetNameNodeFromPath(const std::string& path, std::string* err);

  /// S3A access key retrieved by running command in Init().
  /// If either s3a_secret_key_ or this are empty, the default value is taken from the
  /// local Hadoop client configuration.
  static std::string s3a_access_key_;

  /// S3A secret key retrieved by running command in Init().
  /// If either s3a_access_key_ or this are empty, the default value is taken from the
  /// local Hadoop client configuration.
  static std::string s3a_secret_key_;

 private:
  /// Singleton instance. Instantiated in Init().
  static boost::scoped_ptr<HdfsFsCache> instance_;

  std::mutex lock_; // protects fs_map_
  HdfsFsMap fs_map_;

  HdfsFsCache() { }
  HdfsFsCache(HdfsFsCache const& l); // disable copy ctor
  HdfsFsCache& operator=(HdfsFsCache const& l); // disable assignment
};

}
