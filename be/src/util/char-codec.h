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

#include <string>

#include "common/status.h"

namespace impala {

class MemPool;
class MemTracker;
class ScannerContext;

/// Class for encoding and decoding character buffers between different encodings and
/// UTF-8. Empolys the Boost.Locale library for encoding and decoding.
class CharCodec {
 public:
  static const int MAX_SYMBOL;

  CharCodec(MemPool* memory_pool, const std::string& encoding, char tuple_delim = '\n',
    bool reuse_buffer = false);

  /// Decodes 'buffer' from 'encoding_' to UTF-8, handling partial symbols and delimiters.
  ///
  /// The function processes the buffer in three parts:
  /// 1. Prefix: attempts to complete partial_symbol_, stored from previous DecodeBuffer
  /// call, by adding first bytes from buffer one by one.
  /// 2. Core: Converts the main part of the buffer up to the last delimiter found.
  /// 3. Suffix: in case buffer is split in the middle of a symbol, progressively
  /// determines the incomplete part and stores it into partial_symbol_.
  Status DecodeBuffer(uint8_t** buffer, int64_t* bytes_read, MemPool* pool, bool eosr,
      bool decompress, ScannerContext* context);

  /// Encodes 'str' from UTF-8 into a given 'encoding_'. Since
  /// HdfsTextTableWriter::Flush(), currently being the only client of this function,
  /// always flushes the buffer at the end of the row, we don't need to handle partial
  /// symbols here.
  Status EncodeBuffer(const std::string& str, std::string* result);

 private:
  Status HandlePrefix(uint8_t** buf_start, uint8_t* buf_end, std::string* result_prefix);
  Status HandleCore(uint8_t** buf_start, uint8_t* buf_end, std::string* result_core);
  Status HandleSuffix(uint8_t** buf_start, uint8_t* buf_end, std::string* result_suffix);

  /// Pool to allocate the buffer to hold transformed data.
  MemPool* memory_pool_ = nullptr;

  /// Name of the encoding of the input / output data.
  std::string encoding_;

  /// The following members are only used by DecodeBuffer:
  /// Delimiter used to separate tuples.
  const char tuple_delim_;

  /// Buffer to hold the partial symbol that could not be decoded in the previous call to
  /// DecodeBuffer.
  std::vector<uint8_t> partial_symbol_;

  /// Can we reuse the output buffer or do we need to allocate on each call?
  bool reuse_buffer_;

  /// Buffer to hold transformed data.
  uint8_t* out_buffer_ = nullptr;

  /// Length of the output buffer.
  int64_t buffer_length_ = 0;
};
}
