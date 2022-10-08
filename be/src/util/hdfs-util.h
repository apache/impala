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

#include <string>
#include <hdfs.h>

#include "common/status.h"

namespace impala {

/// Define prefix of remote file systems
extern const char* FILESYS_PREFIX_HDFS;
extern const char* FILESYS_PREFIX_S3;
extern const char* FILESYS_PREFIX_ABFS;
extern const char* FILESYS_PREFIX_ABFS_SEC;
extern const char* FILESYS_PREFIX_ADL;
extern const char* FILESYS_PREFIX_GCS;
extern const char* FILESYS_PREFIX_COS;
extern const char* FILESYS_PREFIX_OZONE;
extern const char* FILESYS_PREFIX_OFS;

/// Utility function to get error messages from HDFS. This function takes prefix/file and
/// appends errno to it. Note: any stdlib function can reset errno, this should be called
/// immediately following the failed call into libhdfs.
std::string GetHdfsErrorMsg(const std::string& prefix, const std::string& file = "");

/// Return the size, in bytes, of a file from the hdfs connection.
Status GetFileSize(const hdfsFS& connection, const char* filename, int64_t* filesize);

/// Returns the last modification time of 'filename' in seconds.
/// This should not be called in a fast path (e.g., running a UDF).
Status GetLastModificationTime(const hdfsFS& connection, const char* filename,
                               time_t* last_mod_time);

bool IsHiddenFile(const std::string& filename);

/// Copy the file at 'src_path' from 'src_conn' to 'dst_path' in 'dst_conn'.
Status CopyHdfsFile(const hdfsFS& src_conn, const std::string& src_path,
                    const hdfsFS& dst_conn, const std::string& dst_path);

/// Returns true iff the path refers to a location on an HDFS filesystem.
/// If check_default_fs is true, the function checks and returns true if the default
/// filesystem is HDFS when the path doen't contain any prefix like 'hdfs://'.
/// If check_default_fs is false, the fucntion checks the path only.
bool IsHdfsPath(const char* path, bool check_default_fs = true);

/// Returns true iff the path refers to a location on an S3A filesystem.
bool IsS3APath(const char* path, bool check_default_fs = true);

/// Returns true iff the path refers to a location on an ABFS filesystem.
bool IsABFSPath(const char* path, bool check_default_fs = true);

/// Returns true iff the path refers to a location on an ADL filesystem.
bool IsADLSPath(const char* path, bool check_default_fs = true);

/// Returns true iff the path refers to a location on an ADL filesystem.
bool IsOSSPath(const char* path, bool check_default_fs = true);

/// Returns true iff the path refers to a location on an GCS filesystem.
bool IsGcsPath(const char* path, bool check_default_fs = true);

/// Returns true iff the path refers to a location on an COS filesystem.
bool IsCosPath(const char* path, bool check_default_fs = true);

/// Returns true iff the path refers to a location on an Ozone filesystem.
bool IsOzonePath(const char* path, bool check_default_fs = true);

/// Returns true iff the path refers to a location on an SFS filesystem.
bool IsSFSPath(const char* path, bool check_default_fs = true);

/// Returns true iff the path refers to a location on an OBS filesystem.
bool IsOBSPath(const char* path, bool check_default_fs = true);

/// Returns true iff 'pathA' and 'pathB' are on the same filesystem and bucket.
/// Most filesystems embed bucket in the authority, but Ozone's ofs protocol allows
/// addressing volume/bucket via the path and does not allow renames across them.
bool FilesystemsAndBucketsMatch(const char* pathA, const char* pathB);

/// Returns the terminal component of 'path'.
/// E.g. if 'path' is "hdfs://localhost:8020/a/b/c", "c" is returned.
/// If the terminal component is empty string or "/", the function returns ".".
std::string GetBaseName(const char* path);
}
