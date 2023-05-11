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

static const int TUPLE_TEXT_FILE_SIZE_ERROR = -1;

/// The TupleTextFileReader is responsible for reading text files created by
/// TupleTextFileWriter line by line. It opens the specified file and allows
/// for sequential access to the content. This class does not support
/// multithreaded access, meaning that all operations should be performed
/// in a single thread to avoid unexpected behaviors.
class TupleTextFileReader {
 public:
  TupleTextFileReader(const std::string& path);
  ~TupleTextFileReader() {}

  // Opens the file for reading.
  Status Open();

  // Closes the file if it is open.
  void Close();

  // Returns the size of the file in bytes.
  int GetFileSize() const;

  // Resets the stream position to the beginning of the file.
  // Should be called before GetNext() if someone wants to ensure starting
  // reading from the beginning.
  void Rewind();

  // Reads one line from the file.
  Status GetNext(string* output, bool* eos);

  const string& GetPath() const { return path_; }

 private:
  // Destination path.
  const std::string path_;
  size_t file_size_ = 0;
  std::ifstream reader_;
};
} // namespace impala
