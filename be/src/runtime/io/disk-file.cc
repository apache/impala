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

#include "runtime/io/disk-file.h"
#include <iostream>
#include <mutex>
#include <thread>
#include "common/names.h"
#include "runtime/io/local-file-writer.h"
#include "util/filesystem-util.h"
#include "util/spinlock.h"

using namespace impala;
using namespace impala::io;

static const Status& DISK_FILE_DELETE_FAILED_INCORRECT_STATUS = Status(ErrorMsg::Init(
    TErrorCode::GENERAL, "DiskFile::Delete() failed with incorrect status"));

Status DiskFile::Delete(const unique_lock<shared_mutex>& lock) {
  DCHECK(lock.mutex() == &physical_file_lock_ && lock.owns_lock());
  Status status = Status::OK();
  unique_lock<SpinLock> status_l(status_lock_);
  // No support for remote file deletion yet.
  if (disk_type_ == DiskFileType::LOCAL_BUFFER || disk_type_ == DiskFileType::LOCAL) {
    if (is_deleted(status_l)) return DISK_FILE_DELETE_FAILED_INCORRECT_STATUS;
    RETURN_IF_ERROR(FileSystemUtil::RemovePaths({path_}));
    SetStatusLocked(io::DiskFileStatus::DELETED, status_l);
  }
  return status;
}

DiskFile::DiskFile(const string& path, DiskIoMgr* io_mgr)
  : path_(path),
    disk_type_(DiskFileType::LOCAL),
    file_status_(DiskFileStatus::INWRITING),
    file_writer_(new LocalFileWriter(io_mgr, path.c_str())),
    space_reserved_(true) {}

DiskFile::DiskFile(const string& path, DiskIoMgr* io_mgr, int64_t file_size,
    DiskFileType disk_type, const hdfsFS* hdfs_conn)
  : path_(path),
    file_size_(file_size),
    disk_type_(disk_type),
    file_status_(DiskFileStatus::INWRITING) {
  DCHECK(disk_type != DiskFileType::LOCAL);
  if (disk_type == DiskFileType::LOCAL_BUFFER) {
    file_writer_.reset(new LocalFileWriter(io_mgr, path.c_str(), file_size));
    hdfs_conn_ = nullptr;
    space_reserved_.Store(false);
  } else {
    DCHECK(hdfs_conn != nullptr);
    hdfs_conn_ = *hdfs_conn;
    space_reserved_.Store(true);
  }
}
