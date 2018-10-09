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

#include "util/hdfs-util.h"

#include <sstream>
#include <string.h>

#include "util/error-util.h"

#include "common/names.h"
#include "runtime/exec-env.h"
#include "kudu/util/path_util.h"

namespace impala {

string GetHdfsErrorMsg(const string& prefix, const string& file) {
  string error_msg = GetStrErrMsg();
  stringstream ss;
  ss << prefix << file << "\n" << error_msg;
  char* root_cause = hdfsGetLastExceptionRootCause();
  if (root_cause != nullptr) {
    ss << "\nRoot cause: " << root_cause;
  }
  return ss.str();
}

Status GetFileSize(const hdfsFS& connection, const char* filename, int64_t* filesize) {
  hdfsFileInfo* info = hdfsGetPathInfo(connection, filename);
  if (info == NULL) return Status(GetHdfsErrorMsg("Failed to get file info ", filename));
  *filesize = info->mSize;
  hdfsFreeFileInfo(info, 1);
  return Status::OK();
}

Status GetLastModificationTime(const hdfsFS& connection, const char* filename,
                               time_t* last_mod_time) {
  hdfsFileInfo* info = hdfsGetPathInfo(connection, filename);
  if (info == NULL) return Status(GetHdfsErrorMsg("Failed to get file info ", filename));
  *last_mod_time = info->mLastMod;
  hdfsFreeFileInfo(info, 1);
  return Status::OK();
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
  return Status::OK();
}

bool IsHdfsPath(const char* path) {
  if (strstr(path, ":/") == NULL) {
    return ExecEnv::GetInstance()->default_fs().compare(0, 7, "hdfs://") == 0;
  }
  return strncmp(path, "hdfs://", 7) == 0;
}

bool IsS3APath(const char* path) {
  if (strstr(path, ":/") == NULL) {
    return ExecEnv::GetInstance()->default_fs().compare(0, 6, "s3a://") == 0;
  }
  return strncmp(path, "s3a://", 6) == 0;
}

bool IsABFSPath(const char* path) {
  if (strstr(path, ":/") == NULL) {
    return ExecEnv::GetInstance()->default_fs().compare(0, 7, "abfs://") == 0 ||
        ExecEnv::GetInstance()->default_fs().compare(0, 8, "abfss://") == 0;
  }
  return strncmp(path, "abfs://", 7) == 0 || strncmp(path, "abfss://", 8) == 0;
}

bool IsADLSPath(const char* path) {
  if (strstr(path, ":/") == NULL) {
    return ExecEnv::GetInstance()->default_fs().compare(0, 6, "adl://") == 0;
  }
  return strncmp(path, "adl://", 6) == 0;
}

// Returns the length of the filesystem name in 'path' which is the length of the
// 'scheme://authority'. Returns 0 if the path is unqualified.
static int GetFilesystemNameLength(const char* path) {
  // Special case for "file:/". It will not have an authority following it.
  if (strncmp(path, "file:", 5) == 0) return 5;

  const char* after_scheme = strstr(path, "://");
  if (after_scheme == NULL) return 0;
  // Some paths may come only with a scheme. We add 3 to skip over "://".
  if (*(after_scheme + 3) == '\0') return strlen(path);

  const char* after_authority = strstr(after_scheme + 3, "/");
  if (after_authority == NULL) return strlen(path);
  return after_authority - path;
}

bool FilesystemsMatch(const char* path_a, const char* path_b) {
  int fs_a_name_length = GetFilesystemNameLength(path_a);
  int fs_b_name_length = GetFilesystemNameLength(path_b);

  const char* default_fs = ExecEnv::GetInstance()->default_fs().c_str();
  int default_fs_name_length = GetFilesystemNameLength(default_fs);

  // Neither is fully qualified: both are on default_fs.
  if (fs_a_name_length == 0 && fs_b_name_length == 0) return true;
  // One is a relative path: check fully-qualified one against default_fs.
  if (fs_a_name_length == 0) {
    DCHECK_GT(fs_b_name_length, 0);
    return strncmp(path_b, default_fs, default_fs_name_length) == 0;
  }
  if (fs_b_name_length == 0) {
    DCHECK_GT(fs_a_name_length, 0);
    return strncmp(path_a, default_fs, default_fs_name_length) == 0;
  }
  DCHECK_GT(fs_a_name_length, 0);
  DCHECK_GT(fs_b_name_length, 0);
  // Both fully qualified: check the filesystem prefix.
  if (fs_a_name_length != fs_b_name_length) return false;
  return strncmp(path_a, path_b, fs_a_name_length) == 0;
}

string GetBaseName(const char* path) {
  int fs_name_length = GetFilesystemNameLength(path);
  if (fs_name_length >= strlen(path)) return ".";

  string bname = kudu::BaseName(&(path[fs_name_length]));
  if (bname.empty() || bname == "/") return ".";

  return bname;
}

}
