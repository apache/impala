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

#include "runtime/io/file-reader.h"

namespace impala {
namespace io {

/// File reader class for the local file system.
/// It uses the standard C APIs from stdio.h
class LocalFileReader : public FileReader {
 public:
  LocalFileReader(ScanRange* scan_range) : FileReader(scan_range) {}
  ~LocalFileReader() {}

  virtual Status Open() override;
  virtual Status ReadFromPos(DiskQueue* disk_queue, int64_t file_offset, uint8_t* buffer,
      int64_t bytes_to_read, int64_t* bytes_read, bool* eof) override;
  /// We don't cache files of the local file system.
  virtual void CachedFile(uint8_t** data, int64_t* length) override;
  virtual void Close() override;

 private:
  /// Points to a C FILE object between calls to Open() and Close(), otherwise nullptr.
  FILE* file_ = nullptr;
};

}
}
