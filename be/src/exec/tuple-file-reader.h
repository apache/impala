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

#include <fstream>
#include <memory>

#include "runtime/bufferpool/buffer-pool.h"
#include "util/runtime-profile.h"

#include "common/status.h"

namespace kudu {
  class RWFile;
}

namespace impala {

class RowBatch;
class RuntimeState;

/// The TupleFileReader reads tuple files produced by the TupleFileWriter. This is used
/// by the tuple cache for reading content from the cache on local disk. The file format
/// is based on the standard RowBatch serialization used for KRPC data streams. The
/// reader relies on the caller using the same RowBatch layout and batch size as the
/// writer.
///
/// In future, the file format should contain enough metadata to dump the contents
/// with no extra information.
///
/// See tuple-file-writer.h for more information.
class TupleFileReader {
public:
  TupleFileReader(const std::string& path, MemTracker* parent, RuntimeProfile* profile);

  ~TupleFileReader();

  // Open the file. This returns an error if there is any issue accessing the file
  Status Open(RuntimeState *state);

  // Read a row batch from the file. Sets eos=true if it reaches the end of the file.
  Status GetNext(RuntimeState *state, BufferPool::ClientHandle* bpclient,
      RowBatch* output_row_batch, bool* eos);

 private:
  // Path to read.
  std::string path_;
  // MemTracker for deserializing protobuf.
  std::shared_ptr<MemTracker> tracker_;
  // Total read time by the reader.
  RuntimeProfile::Counter* read_timer_;
  // Total time spent on deserialization.
  RuntimeProfile::Counter* deserialize_timer_;

  std::unique_ptr<kudu::RWFile> reader_;
  size_t offset_ = 0;
  size_t file_size_ = 0;
};

}
