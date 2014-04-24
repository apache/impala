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

#include "util/hdfs-util.h"

#include <sstream>
#include <string.h>

#include "util/error-util.h"

using namespace boost;
using namespace std;

namespace impala {

string GetHdfsErrorMsg(const string& prefix, const string& file) {
  string error_msg = GetStrErrMsg();
  stringstream ss;
  ss << prefix << file << "\n" << error_msg;
  return ss.str();
}

Status GetFileSize(const hdfsFS& connection, const char* filename, int64_t* filesize) {
  hdfsFileInfo* info = hdfsGetPathInfo(connection, filename);
  if (info == NULL) return Status(GetHdfsErrorMsg("Failed to get file info ", filename));
  *filesize = info->mSize;
  hdfsFreeFileInfo(info, 1);
  return Status::OK;
}

Status GetLastModificationTime(const hdfsFS& connection, const char* filename,
                               time_t* last_mod_time) {
  hdfsFileInfo* info = hdfsGetPathInfo(connection, filename);
  if (info == NULL) return Status(GetHdfsErrorMsg("Failed to get file info ", filename));
  *last_mod_time = info->mLastMod;
  hdfsFreeFileInfo(info, 1);
  return Status::OK;
}

bool IsHiddenFile(const string& filename) {
  return !filename.empty() && (filename[0] == '.' || filename[0] == '_');
}

Status CopyHdfsFile(const hdfsFS& src_conn, const string& src_path,
                    const hdfsFS& dst_conn, const string& dst_path) {
  int error = hdfsCopy(src_conn, src_path.c_str(), dst_conn, dst_path.c_str());
  if (error != 0) {
    string error_msg = GetHdfsErrorMsg("");
    stringstream ss;
    ss << "Failed to copy " << src_path << " to " << dst_path << ": " << error_msg;
    return Status(ss.str());
  }
  return Status::OK;
}

}
