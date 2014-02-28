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
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/fe-test-info.h"

using namespace std;
using namespace boost;

namespace impala {

scoped_ptr<HdfsFsCache> HdfsFsCache::instance_;

void HdfsFsCache::Init() {
  DCHECK(HdfsFsCache::instance_.get() == NULL);
  HdfsFsCache::instance_.reset(new HdfsFsCache());
}

hdfsFS HdfsFsCache::GetConnection(const string& host, int port) {
  lock_guard<mutex> l(lock_);
  HdfsFsMap::iterator i = fs_map_.find(make_pair(host, port));
  if (i == fs_map_.end()) {
    hdfsBuilder* hdfs_builder = hdfsNewBuilder();
    if (!host.empty()) {
      hdfsBuilderSetNameNode(hdfs_builder, host.c_str());
    } else {
      // Connect to local filesystem
      hdfsBuilderSetNameNode(hdfs_builder, NULL);
    }
    hdfsBuilderSetNameNodePort(hdfs_builder, port);
    hdfsFS conn = hdfsBuilderConnect(hdfs_builder);
    DCHECK(conn != NULL);
    fs_map_.insert(make_pair(make_pair(host, port), conn));
    return conn;
  } else {
    return i->second;
  }
}

hdfsFS HdfsFsCache::GetDefaultConnection() {
  // "default" uses the default NameNode configuration from the XML configuration files.
  return GetConnection("default", 0);
}

hdfsFS HdfsFsCache::GetLocalConnection() {
  return GetConnection("", 0);
}

}
