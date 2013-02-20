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

#include "runtime/hdfs-fs-cache.h"

#include <boost/thread/locks.hpp>

#include "common/logging.h"

using namespace std;
using namespace boost;

// TODO: Consider retiring these altogether once reading from Hadoop
// config has proven itself
DEFINE_string(nn, "", "hostname or ip address of HDFS namenode. If not explicitly set, "
              "Impala will read the value from its Hadoop configuration files.");
DEFINE_int32(nn_port, 0, "namenode port. If -nn is not explicitly set, Impala will read "
             "the value from its Hadoop configuration files");

namespace impala {

HdfsFsCache::~HdfsFsCache() {
  for (HdfsFsMap::iterator i = fs_map_.begin(); i != fs_map_.end(); ++i) {
    int status = hdfsDisconnect(i->second);
    if (status != 0) {
      // TODO: add error details
      LOG(ERROR) << "hdfsDisconnect(\"" << i->first.first << "\", " << i->first.second
                 << ") failed: " << " Error(" << errno << "): " << strerror(errno);
    }
  }
}

hdfsFS HdfsFsCache::GetConnection(const string& host, int port) {
  lock_guard<mutex> l(lock_);
  HdfsFsMap::iterator i = fs_map_.find(make_pair(host, port));
  if (i == fs_map_.end()) {
    hdfsFS conn = hdfsConnect(host.c_str(), port);
    DCHECK(conn != NULL);
    fs_map_.insert(make_pair(make_pair(host, port), conn));
    return conn;
  } else {
    return i->second;
  }
}

hdfsFS HdfsFsCache::GetDefaultConnection() {
  return GetConnection(FLAGS_nn, FLAGS_nn_port);
}

}
