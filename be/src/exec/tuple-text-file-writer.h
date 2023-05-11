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

#include "common/status.h"

namespace impala {

class RowBatch;

/// The TupleTextFileWriter is responsible for serializing a stream of RowBatches to a
/// local text file for the tuple cache. It uses a human-readable format to represent
/// the rows, making the data accessible and easy to interpret.
///
/// The TupleTextFileWriter writes to a specified location. Commit() is to ensure that
/// the file persists beyond the life of the writer.
/// If the TupleTextFileWriter is destructed without calling Commit(), it automatically
/// persists the file.
///
/// This class does not support multithreaded access, meaning that all operations should
/// be performed in a single thread to avoid unexpected behaviors.
class TupleTextFileWriter {
 public:
  TupleTextFileWriter(std::string path);
  ~TupleTextFileWriter();

  Status Open();

  // Writes a row batch to file. This holds no references to memory from the RowBatch.
  // If Write() returns a non-OK Status, it is not recoverable and the caller should not
  // call Write() or Commit().
  Status Write(RowBatch* row_batch);

  // Return true if nothing is written.
  bool IsEmpty() const;

  // Number of bytes written to file. Must be called before Commit/Delete.
  size_t BytesWritten() const { return bytes_written_; }

  // Stop writing and delete written data.
  void Delete();

  // Ensure data is available for future reads.
  void Commit();

  const std::string GetPath() const { return path_; }

 private:
  // Destination path
  const std::string path_;
  // Bytes of written data
  size_t bytes_written_;
  std::ofstream writer_;
};

} // namespace impala
