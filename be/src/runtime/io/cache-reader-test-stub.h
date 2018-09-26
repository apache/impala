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
#include "runtime/io/request-ranges.h"

namespace impala {
namespace io {

/// Only for testing the code path when reading from the cache is successful.
/// Takes a pointer to a buffer in its constructor, also the length of this buffer.
/// CachedFile() simply returns the pointer and length.
/// Invoking ReadFromPos() on it results in an error.
class CacheReaderTestStub : public FileReader {
public:
  CacheReaderTestStub(ScanRange* scan_range, uint8_t* cache, int64_t length) :
    FileReader(scan_range),
    cache_(cache),
    length_(length) {
  }

  ~CacheReaderTestStub() {}

  virtual Status Open(bool use_file_handle_cache) override {
    return Status::OK();
  }

  virtual Status ReadFromPos(int64_t file_offset, uint8_t* buffer,
      int64_t bytes_to_read, int64_t* bytes_read, bool* eof) override {
    DCHECK(false);
    return Status("Not implemented");
  }

  virtual void CachedFile(uint8_t** data, int64_t* length) override {
    *length = length_;
    *data = cache_;
  }

  virtual void Close() override {}
private:
  uint8_t* cache_ = nullptr;
  int64_t length_ = 0;
};

}
}
