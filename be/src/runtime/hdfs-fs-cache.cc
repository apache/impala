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
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/hdfs-util.h"
#include "util/test-info.h"

using namespace std;
using namespace boost;
using namespace strings;

namespace impala {

scoped_ptr<HdfsFsCache> HdfsFsCache::instance_;

void HdfsFsCache::Init() {
  DCHECK(HdfsFsCache::instance_.get() == NULL);
  HdfsFsCache::instance_.reset(new HdfsFsCache());
}

Status HdfsFsCache::GetConnection(const string& path, hdfsFS* fs) {
  string namenode;
  size_t n = path.find("://");
  if (n == string::npos) {
    // Path is not qualified, so use the default FS.
    namenode = "default";
  } else {
    // Path is qualified, i.e. "scheme://authority/path/to/file".  Extract
    // "scheme://authority/".
    n = path.find('/', n + 3);
    if (n == string::npos) {
      return Status(Substitute("Path missing '/' after authority: $0", path));
    }
    // Include the trailling '/' for local filesystem case, i.e. "file:///".
    namenode = path.substr(0, n + 1);
  }
  DCHECK(!namenode.empty());

  lock_guard<mutex> l(lock_);
  HdfsFsMap::iterator i = fs_map_.find(namenode);
  if (i == fs_map_.end()) {
    hdfsBuilder* hdfs_builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(hdfs_builder, namenode.c_str());
    *fs = hdfsBuilderConnect(hdfs_builder);
    if (*fs == NULL) {
      return Status(GetHdfsErrorMsg("Failed to connect to FS: ", namenode));
    }
    fs_map_.insert(make_pair(namenode, *fs));
  } else {
    *fs = i->second;
  }
  DCHECK_NOTNULL(*fs);
  return Status::OK;
}

Status HdfsFsCache::GetLocalConnection(hdfsFS* fs) {
  return GetConnection("file:///", fs);
}

}
