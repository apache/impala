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

#include "common/logging.h"
#include "kudu/util/path_util.h"
#include "runtime/exec-env.h"
#include "util/error-util.h"

#include "common/names.h"

namespace impala {

const char* FILESYS_PREFIX_HDFS = "hdfs://";
const char* FILESYS_PREFIX_S3 = "s3a://";
const char* FILESYS_PREFIX_ABFS = "abfs://";
const char* FILESYS_PREFIX_ABFS_SEC = "abfss://";
const char* FILESYS_PREFIX_ADL = "adl://";
const char* FILESYS_PREFIX_GCS = "gs://";
const char* FILESYS_PREFIX_COS = "cosn://";
const char* FILESYS_PREFIX_OZONE = "o3fs://";
const char* FILESYS_PREFIX_OFS = "ofs://";
const char* FILESYS_PREFIX_SFS = "sfs+";
const char* FILESYS_PREFIX_OSS = "oss://";
const char* FILESYS_PREFIX_JINDOFS = "jfs://";
const char* FILESYS_PREFIX_OBS = "obs://";

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

static bool IsSpecificPath(
    const char* path, const char* specific_prefix, bool check_default_fs) {
  size_t prefix_len = strlen(specific_prefix);
  if (check_default_fs && strstr(path, ":/") == NULL) {
    return strncmp(
               ExecEnv::GetInstance()->default_fs().c_str(), specific_prefix, prefix_len)
        == 0;
  }
  return strncmp(path, specific_prefix, prefix_len) == 0;
}

bool IsHdfsPath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_HDFS, check_default_fs);
}

bool IsS3APath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_S3, check_default_fs);
}

bool IsABFSPath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_ABFS, check_default_fs)
      || IsSpecificPath(path, FILESYS_PREFIX_ABFS_SEC, check_default_fs);
}

bool IsADLSPath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_ADL, check_default_fs);
}

bool IsOSSPath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_OSS, check_default_fs)
      || IsSpecificPath(path, FILESYS_PREFIX_JINDOFS, check_default_fs);
}

bool IsGcsPath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_GCS, check_default_fs);
}

// o3fs and ofs uses the same transport implementation, so they should share
// the same thread pool.
bool IsCosPath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_COS, check_default_fs);
}

bool IsOzonePath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_OZONE, check_default_fs)
      || IsSpecificPath(path, FILESYS_PREFIX_OFS, check_default_fs);
}

bool IsSFSPath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_SFS, check_default_fs);
}

bool IsOBSPath(const char* path, bool check_default_fs) {
  return IsSpecificPath(path, FILESYS_PREFIX_OBS, check_default_fs);
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

static bool FilesystemsMatch(const char* path_a, const char* path_b) {
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

static int VolumeBucketLength(const char* path) {
  if (*path == '\0') return 0;
  const char* afterVolume = strstr(path, "/");
  if (afterVolume == nullptr) return strlen(path);
  const char* afterBucket = strstr(afterVolume + 1, "/");
  if (afterBucket == nullptr) return strlen(path);
  return afterBucket - path;
}

static bool OfsBucketsMatch(const char* path_a, const char* path_b) {
  // Examine only the path elements.
  path_a = path_a + GetFilesystemNameLength(path_a);
  path_b = path_b + GetFilesystemNameLength(path_b);
  // Skip past starting slash for comparison to unqualified paths.
  if (*path_a == '/') ++path_a;
  if (*path_b == '/') ++path_b;

  int vba_len = VolumeBucketLength(path_a);
  int vbb_len = VolumeBucketLength(path_b);
  if (vba_len != vbb_len) return false;
  return strncmp(path_a, path_b, vba_len) == 0;
}

bool FilesystemsAndBucketsMatch(const char* path_a, const char* path_b) {
  if (!FilesystemsMatch(path_a, path_b)) return false;

  // path_a and path_b are in the same filesystem, so we just need to check one prefix.
  if (IsSpecificPath(path_a, FILESYS_PREFIX_OFS, true)) {
    return OfsBucketsMatch(path_a, path_b);
  }
  return true;
}

string GetBaseName(const char* path) {
  int fs_name_length = GetFilesystemNameLength(path);
  if (fs_name_length >= strlen(path)) return ".";

  string bname = kudu::BaseName(&(path[fs_name_length]));
  if (bname.empty() || bname == "/") return ".";

  return bname;
}

}
