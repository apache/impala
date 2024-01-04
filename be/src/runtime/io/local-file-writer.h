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

#include "runtime/io/file-writer.h"
#include "runtime/tmp-file-mgr-internal.h"

namespace impala {
namespace io {

/// File writer class for the local file system.
class LocalFileWriter : public FileWriter {
 public:
  LocalFileWriter(DiskIoMgr* io_mgr, const char* file_path, int64_t file_size = 0)
    : FileWriter(io_mgr, file_path, file_size) {}
  ~LocalFileWriter();

  virtual Status Open() override;
  virtual Status Write(WriteRange* range, int64_t* written_bytes) override;
  virtual Status Close() override;

  // Open and close the file within the function.
  virtual Status WriteOne(WriteRange* range) override;

 private:
  /// Points to a C FILE object between calls to Open() and Close(), otherwise nullptr.
  FILE* file_ = nullptr;
};
} // namespace io
} // namespace impala
