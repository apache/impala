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
#include <boost/filesystem.hpp>

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

bool IsHiddenFile(const string& filename) {
  return !filename.empty() && (filename[0] == '.' || filename[0] == '_');
}

Status CopyHdfsFile(const hdfsFS& src_conn, const char* src_path,
                    const hdfsFS& dst_conn, const char* dst_dir,
                    string* dst_path) {

  // Append the source filename to the destination directory. Also add the process ID to
  // the filename so multiple processes don't clobber each other's files.
  filesystem::path src(src_path);
  stringstream dst;
  dst << dst_dir << "/" << src.stem().native() << "." << getpid()
      << src.extension().native();
  *dst_path = dst.str();

  int error = hdfsCopy(src_conn, src_path, dst_conn, dst_path->c_str());
  if (error != 0) {
    string error_msg = GetHdfsErrorMsg("");
    stringstream ss;
    ss << "Failed to copy " << src_path << " to " << dst_dir << ": " << error_msg;
    return Status(ss.str());
  }
  return Status::OK;
}

}
