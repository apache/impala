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

#include "runtime/io/error-converter.h"
#include "runtime/io/local-file-system.h"
#include "runtime/io/request-ranges.h"

#include <fcntl.h>

namespace impala {
namespace io {

int LocalFileSystem::OpenAux(const char* file, int option1, int option2) {
  return open(file, option1, option2);
}

FILE* LocalFileSystem::FdopenAux(int file_desc, const char* options) {
  return fdopen(file_desc, options);
}

Status LocalFileSystem::OpenForRead(
    const char* file_name, int oflag, int mode, FILE** file) {
  return Open(file_name, oflag, mode, "rb", file);
}

Status LocalFileSystem::OpenForWrite(
    const char* file_name, int oflag, int mode, FILE** file) {
  return Open(file_name, oflag, mode, "wb", file);
}

Status LocalFileSystem::Open(
    const char* file_name, int oflag, int mode, const char* fd_option, FILE** file) {
  DCHECK(file_name != nullptr);
  DCHECK(file != nullptr);

  int file_desc = OpenAux(file_name, oflag, mode);
  if (file_desc < 0) {
    return ErrorConverter::GetErrorStatusFromErrno("open()", file_name, errno);
  }

  *file = FdopenAux(file_desc, fd_option);
  if (*file == nullptr) {
    Status fdopen_status = ErrorConverter::GetErrorStatusFromErrno("fdopen()", file_name,
        errno);
    if (close(file_desc) < 0) {
      fdopen_status.MergeStatus(ErrorConverter::GetErrorStatusFromErrno("close()",
          file_name, errno));
    }
    return fdopen_status;
  }
  return Status::OK();
}

Status LocalFileSystem::Fseek(FILE* file_handle, off_t offset, int whence,
    const WriteRange* write_range) {
  DCHECK(file_handle != nullptr);
  if (FseekAux(file_handle, offset, whence) != 0) {
    return ErrorConverter::GetErrorStatusFromErrno("fseek()", write_range->file(),
        errno, {{"offset", SimpleItoa(offset)}});
  }
  return Status::OK();
}

int LocalFileSystem::FseekAux(FILE* file_handle, off_t offset, int whence){
  return fseek(file_handle, offset, whence);
}

Status LocalFileSystem::Fwrite(FILE* file_handle, const WriteRange* write_range) {
  DCHECK(file_handle != nullptr);
  DCHECK(write_range != nullptr);
  int64_t bytes_written = FwriteAux(file_handle, write_range);
  if (bytes_written < write_range->len()) {
    return ErrorConverter::GetErrorStatusFromErrno("fwrite()", write_range->file(),
        errno, {{"range_length", SimpleItoa(write_range->len())}});
  }
  return Status::OK();
}

size_t LocalFileSystem::FwriteAux(FILE* file_handle, const WriteRange* write_range) {
  return fwrite(write_range->data(), 1, write_range->len(), file_handle);
}

Status LocalFileSystem::Fread(
    FILE* file_handle, uint8_t* buffer, int64_t length, const char* file_path) {
  DCHECK(file_handle != nullptr);
  int64_t bytes_read = FreadAux(file_handle, buffer, length);
  if (bytes_read < length) {
    return ErrorConverter::GetErrorStatusFromErrno(
        "fread()", file_path, errno, {{"range_length", SimpleItoa(length)}});
  }
  return Status::OK();
}

size_t LocalFileSystem::FreadAux(FILE* file_handle, uint8_t* buffer, int64_t length) {
  return fread(buffer, 1, length, file_handle);
}

Status LocalFileSystem::Fclose(FILE* file_handle, const char* file_path) {
  DCHECK(file_handle != nullptr);
  if (FcloseAux(file_handle) != 0) {
    return ErrorConverter::GetErrorStatusFromErrno("fclose()", file_path, errno);
  }
  return Status::OK();
}

int LocalFileSystem::FcloseAux(FILE* file_handle) {
  return fclose(file_handle);
}

Status LocalFileSystem::Write(int file_desc, const WriteRange* range) {
  DCHECK(range != nullptr);
  int64_t bytes_written = write(file_desc, range->data(), range->len());
  if (bytes_written < range->len()) {
    return ErrorConverter::GetErrorStatusFromErrno(
        "write()", range->file(), errno, {{"range_length", SimpleItoa(range->len())}});
  }
  return Status::OK();
}
}
}
