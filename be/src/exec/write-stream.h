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

#include <cstdint>

#include "common/status.h"

namespace impala {

/// An append-only buffer to stage output from file writers. The buffer is backed
/// by a stringstream and uses the ReadWriteUtil to encode data. Append
/// operations will never fail, and will grow the backing buffer using
/// stringstream semantics. Each write function returns the number of bytes written
class WriteStream {
 public:

  WriteStream() : len_(0) { }

  /// Writes bytes to the buffer, returns the number of bytes written
  inline int WriteBytes(int length, const uint8_t* buf);
  inline int WriteBytes(int length, const char* buf);

  inline int WriteVInt(int32_t val);
  inline int WriteInt(uint32_t val);
  inline int WriteByte(uint8_t val);
  inline int WriteByte(char val);
  inline int WriteVLong(int64_t val);
  inline int WriteBoolean(bool val);
  /// Writes a zig-zag encoded integer
  inline int WriteZInt(int32_t val);
  inline int WriteZLong(int64_t val);
  /// Writes the length as a VLong follows by the byte string
  inline int WriteText(int32_t len, const uint8_t* buf);
  /// Writes an empty string to the buffer (encoded as 1 byte)
  inline int WriteEmptyText();

  inline void Clear();
  inline size_t Size();

  /// returns the contents of this stream as a string
  inline std::string String();

 private:
  /// TODO consider making this like the parquet writer to avoid extra copy
  std::stringstream buffer_;
  uint64_t len_;
};

} // namespace impala
