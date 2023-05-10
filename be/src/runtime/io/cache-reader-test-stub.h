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

#include "runtime/io/local-file-reader.h"
#include "runtime/io/request-ranges.h"

namespace impala {
namespace io {

// Test scenarios:
// VALID_BUFFER - Successful HDFS cache read returns the whole buffer / length
// FALLBACK_NULL_BUFFER - Failed HDFS cache read returns null / length = 0
// FALLBACK_INCOMPLETE_BUFFER - Failed HDFS cache read returns invalid buffer
//   with a short length
enum HdfsCachingScenario {
    VALID_BUFFER,
    FALLBACK_NULL_BUFFER,
    FALLBACK_INCOMPLETE_BUFFER
};

/// This simulates reads from HDFS caching for the successful path (VALID_BUFFER)
/// and some unsuccessful paths (FALLBACK*) that should fall back to the normal
/// file read path.
/// Takes a pointer to a buffer, the length of the buffer, and an indicator of
/// what scenario to test. The scenario determins what CachedFile() returns.
/// See the description of HdfsCachingScenario above.
/// All other methods fall through to the LocalFileReader, so fallback reads
/// can succeed for applicable scenarios. The passthrough of Open() and Close()
/// are harmless for the successful cache scenario.
class CacheReaderTestStub : public LocalFileReader {
public:
  CacheReaderTestStub(ScanRange* scan_range, uint8_t* cache, int64_t length,
                      HdfsCachingScenario scenario) :
    LocalFileReader(scan_range),
    cache_(cache),
    length_(length),
    scenario_(scenario){
  }

  ~CacheReaderTestStub() {}

  virtual Status ReadFromPos(DiskQueue* queue, int64_t file_offset,
      uint8_t* buffer, int64_t bytes_to_read, int64_t* bytes_read, bool* eof) override {
    // This should not be reached for the VALID_BUFFER scenario, because
    // the reads will come from the cached buffer.
    DCHECK_NE(scenario_, VALID_BUFFER);
    return LocalFileReader::ReadFromPos(queue, file_offset, buffer, bytes_to_read,
        bytes_read, eof);
  }

  virtual void CachedFile(uint8_t** data, int64_t* length) override {
    switch (scenario_) {
    case VALID_BUFFER:
      *length = length_;
      *data = cache_;
      break;
    case FALLBACK_NULL_BUFFER:
      *length = 0;
      *data = nullptr;
      break;
    case FALLBACK_INCOMPLETE_BUFFER:
      // Use a fake too-short length and fake non-null buffer
      // The buffer should not be dereferenced in this scenario, so
      // it is useful for it to be invalid.
      *length = 1;
      *data = (uint8_t *) 0x1;
      break;
    default:
      DCHECK(false) << "Invalid HdfsCachingScenario value";
    }
  }

private:
  uint8_t* cache_ = nullptr;
  int64_t length_ = 0;
  HdfsCachingScenario scenario_;
};

}
}
