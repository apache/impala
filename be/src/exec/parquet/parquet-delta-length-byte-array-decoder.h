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
#include "exec/parquet/parquet-delta-decoder.h"
#include "runtime/string-value.h"

namespace impala {

/// Decodes DELTA_LENGTH_BYTE_ARRAY encoded Parquet pages (encoding enum 6).
///
/// Page layout:
///   <DELTA_BINARY_PACKED lengths section (INT32 values)>
///   <raw concatenated byte data for all strings>
///
/// Usage per page:
///   1. Call NewPage() with the full page payload (after rep/def levels).
///   2. Call NextValue() / NextValues() / SkipValues() to consume strings.
///
/// Short strings (up to StringValue::SMALL_LIMIT bytes) are smallified into the
/// StringValue's inline storage so the page buffer can be released early.
/// Longer strings point directly into the page buffer (zero-copy); in that case
/// HasOnlySmallStrings() returns false and the caller must keep the buffer alive.
///
/// Lengths are decoded lazily in fixed-size batches (LENGTHS_BATCH_SIZE) rather
/// than all at once, so memory usage is O(1) regardless of the number of values.
class ParquetDeltaLengthByteArrayDecoder {
 public:
  /// Initialise the decoder for a new page. 'data' must point to the start of the
  /// DELTA_LENGTH_BYTE_ARRAY payload and 'data_len' is its total byte length.
  /// 'page_num_values' is the value count from the data page header (including NULLs);
  /// it is used as a trusted upper bound to reject corrupt in-page counts that could
  /// otherwise cause excessive memory use or CPU work.
  /// Returns an error if the header is corrupt or the buffer is too small.
  Status NewPage(const uint8_t* data, int data_len,
      int page_num_values) WARN_UNUSED_RESULT;

  /// Number of string values encoded in this page.
  /// Only valid after a successful call to NewPage().
  int GetTotalValueCount() const {
    DCHECK(initialized_);
    return total_value_count_;
  }

  /// Only valid after a successful call to NewPage().
  int NextValue(StringValue* out) WARN_UNUSED_RESULT;

  /// Decode up to 'num_values' strings, writing them to 'out' with 'stride' bytes
  /// between consecutive StringValues in the output buffer.
  /// Returns the number decoded, 0 if exhausted, -1 on error.
  int NextValues(int num_values, StringValue* out, int64_t stride) WARN_UNUSED_RESULT;

  /// Skip 'num_values' values. Returns the number skipped, 0 if exhausted, -1 on error.
  int SkipValues(int num_values) WARN_UNUSED_RESULT;

  /// Returns true if all strings decoded since the last NewPage() call were
  /// smallified. If false, the caller must keep the page buffer alive for strings
  /// whose length exceeds StringValue::SMALL_LIMIT.
  bool HasOnlySmallStrings() const { return has_only_small_; }

 private:
  /// Refills lengths_buf_ from len_decoder_. Returns number decoded
  /// (0 if exhausted, -1 on error).
  int FillLengthsBuffer();

  bool initialized_ = false;

  /// True if all strings decoded since the last NewPage() were smallified.
  bool has_only_small_ = true;

  /// Total number of string values in this page (set by NewPage()).
  int total_value_count_ = 0;

  /// Persistent delta decoder for streaming length reads.
  ParquetDeltaDecoder<int32_t> len_decoder_;

  /// Fixed-size buffer holding one batch of decoded lengths.
  static constexpr int LENGTHS_BATCH_SIZE = 1024;
  int32_t lengths_buf_[LENGTHS_BATCH_SIZE];

  /// Next unconsumed index in lengths_buf_.
  int buf_pos_ = 0;

  /// Number of valid entries currently in lengths_buf_.
  int buf_filled_ = 0;

  /// Number of length values not yet decoded into the buffer.
  int values_remaining_ = 0;

  /// Pointer to the current position in the concatenated string data section.
  const uint8_t* data_ = nullptr;

  /// Bytes remaining in the string data section.
  int bytes_remaining_ = 0;
};

} // namespace impala
