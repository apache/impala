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
    if (file_writer_ != nullptr) {
      // Close the file writer to release the file handle.
      RETURN_IF_ERROR(file_writer_->Close());
    }
    // Remove the local physical file if it exists.
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

DiskFile::DiskFile(const string& path, DiskIoMgr* io_mgr, int64_t file_size,
    DiskFileType disk_type, int64_t read_buffer_block_size, int num_read_buffer_blocks)
  : path_(path),
    file_size_(file_size),
    disk_type_(disk_type),
    file_status_(DiskFileStatus::INWRITING) {
  DCHECK(disk_type == DiskFileType::LOCAL_BUFFER);
  hdfs_conn_ = nullptr;
  space_reserved_.Store(false);
  file_writer_.reset(new LocalFileWriter(io_mgr, path_.c_str(), file_size));
  read_buffer_ =
      std::make_unique<ReadBuffer>(read_buffer_block_size, num_read_buffer_blocks);
}

DiskFile::ReadBuffer::ReadBuffer(
    int64_t read_buffer_block_size, int64_t num_read_buffer_blocks)
  : read_buffer_block_size_(read_buffer_block_size),
    num_of_read_buffer_blocks_(num_read_buffer_blocks) {
  page_cnts_per_block_ = std::make_unique<int64_t[]>(num_read_buffer_blocks);
  read_buffer_block_offsets_ = std::make_unique<int64_t[]>(num_read_buffer_blocks);
  memset(page_cnts_per_block_.get(), 0, num_read_buffer_blocks * sizeof(int64_t));
  memset(read_buffer_block_offsets_.get(), DISK_FILE_INVALID_FILE_OFFSET,
      num_read_buffer_blocks * sizeof(int64_t));
  for (int i = 0; i < num_read_buffer_blocks; i++) {
    read_buffer_blocks_.emplace_back(std::make_unique<MemBlock>(i));
  }
}

void MemBlock::Delete(bool* reserved, bool* allocated) {
  DCHECK(reserved != nullptr);
  DCHECK(allocated != nullptr);
  *reserved = false;
  *allocated = false;
  unique_lock<SpinLock> lock(mem_block_lock_);
  switch (status_) {
    case MemBlockStatus::WRITTEN:
    case MemBlockStatus::ALLOC:
      // Release the memory.
      DCHECK(data_ != nullptr);
      free(data_);
      data_ = nullptr;
      *allocated = true;
      [[fallthrough]];
    case MemBlockStatus::RESERVED:
      *reserved = true;
      [[fallthrough]];
    default:
      SetStatusLocked(lock, MemBlockStatus::DISABLED);
      DCHECK(data_ == nullptr);
  }
}
