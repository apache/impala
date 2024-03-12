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
#include <limits>
#include <memory>

#include "util/runtime-profile.h"

#include "common/status.h"

namespace kudu {
  class WritableFile;
}

namespace impala {

class MemTracker;
class RowBatch;
class RuntimeState;
class TupleReadWriteTest;

/// The TupleFileWriter is used to serialize a stream of RowBatches to a local file
/// for the tuple cache. It uses the standard RowBatch serialization used for KRPC
/// data streams (i.e. RowBatch::Serialize()). The files can be read back using the
/// TupleFileReader.
///
/// The TupleFileWriter writes in a temporary location and then moves the file
/// into its final location when Commit() is called. Commit() is the only way that a
/// file will persist over time. If the TupleFileWriter is destructed without calling
/// Commit(), it runs Abort() and any associated file is deleted. The user can
/// proactively call Abort() to delete any associated files, but it is not required.
///
/// The TupleFileWriter enforces a maximum file size and will fail Write() calls that
/// would exceed this limit. It provides a way for the caller to get how many bytes
/// have been written for accounting purposes.
///
/// Currently, the TupleFileWriter does not embed the actual tuple layout into the
/// file. It relies on the corresponding TupleFileReader reading with the same
/// tuple layout. This will be modified later to embed a representation of the tuple
/// layout into the file.
class TupleFileWriter {
public:
  TupleFileWriter(std::string path, MemTracker* parent, RuntimeProfile* profile,
      size_t max_file_size = std::numeric_limits<size_t>::max());
  ~TupleFileWriter();

  Status Open(RuntimeState* state);

  // Writes a row batch to file. This holds no references to memory from the RowBatch.
  // If Write() returns a non-OK Status, it is not recoverable and the caller should not
  // call Write() or Commit().
  Status Write(RuntimeState* state, RowBatch* row_batch);

  bool ExceededMaxSize() const { return exceeded_max_size_; }

  // Number of bytes written to file. Must be called before Commit/Abort.
  size_t BytesWritten() const;

  // Stop writing and delete any written data.
  void Abort();

  // Ensure data is available for future reads.
  Status Commit(RuntimeState* state);

protected:
  friend class TupleFileReadWriteTest;

  std::string TempPath() const;

private:
  // Destination path
  std::string path_;
  // Suffix for temporary filename during writing.
  std::string temp_suffix_;
  // MemTracker for OutboundRowBatches.
  std::shared_ptr<MemTracker> tracker_;
  // Total write time by the writer.
  RuntimeProfile::Counter* write_timer_;
  // Total time spent on serialization.
  RuntimeProfile::Counter* serialize_timer_;
  // Maximum size for the resulting file
  size_t max_file_size_;
  // True if the file reached the maximum size
  bool exceeded_max_size_ = false;

  // This writes to a temporary file, only moving it into the final location with
  // Commit(). tmp_file_ is the file abstraction used for writing the temporary file.
  std::unique_ptr<kudu::WritableFile> tmp_file_;

  // The writer starts as UNINIT. Once it transitions to IN_PROGRESS in Open(), it will
  // be ABORTED unless the caller runs Commit().
  enum class State {
     Uninitialized,
     InProgress,
     Committed,
     Aborted
  };

  friend
  std::ostream& operator<<(std::ostream& out, const TupleFileWriter::State& state);

  State state_ = State::Uninitialized;
};

}
