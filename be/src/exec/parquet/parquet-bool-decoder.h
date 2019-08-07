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

#include "common/compiler-util.h"
#include "exec/parquet/parquet-common.h"
#include "util/mem-util.h"
#include "util/rle-encoding.h"

namespace impala {

/// Decoder for RLE and bit-packed boolean-encoded values.
class ParquetBoolDecoder {
 public:
  /// Set the data for the next page to decode. Return true if the encoding is supported,
  /// false otherwise.
  bool SetData(parquet::Encoding::type encoding, uint8_t* data, int size);

  /// Decode the next bool value to 'value'. Return true on success or false if there is
  /// an error decoding, e.g. invalid or truncated data.
  /// Templated so that callers can avoid the overhead of branching on encoding per row.
  template <parquet::Encoding::type ENCODING>
  bool ALWAYS_INLINE DecodeValue(bool* RESTRICT value) RESTRICT;

  /// Batched version of DecodeValue() that decodes multiple values at a time.
  bool DecodeValues(int64_t stride, int64_t count, bool* RESTRICT first_value) RESTRICT;

  /// Skip 'num_values' values from the column data.
  ///TODO: add e2e tests when page filtering is implemented (IMPALA-5843).
  bool SkipValues(int num_values) RESTRICT;

 private:
  /// Implementation of DecodeValues, templated by ENCODING.
  template <parquet::Encoding::type ENCODING>
  bool DecodeValues(int64_t stride, int64_t count, bool* RESTRICT first_value) RESTRICT;

  parquet::Encoding::type encoding_;

  /// A buffer to store unpacked values. Must be a multiple of 32 size to use the
  /// batch-oriented interface of BatchedBitReader. We use uint8_t instead of bool because
  /// bit unpacking is only supported for unsigned integers. The values are converted to
  /// bool when returned to the user.
  static const int UNPACKED_BUFFER_LEN = 128;
  uint8_t unpacked_values_[UNPACKED_BUFFER_LEN];

  /// The number of valid values in 'unpacked_values_'.
  int num_unpacked_values_ = 0;

  /// The next value to return from 'unpacked_values_'.
  int unpacked_value_idx_ = 0;

  /// Bit packed decoder, used if 'encoding_' is PLAIN.
  BatchedBitReader bool_values_;

  /// RLE decoder, used if 'encoding_' is RLE.
  RleBatchDecoder<uint8_t> rle_decoder_;
};

template <parquet::Encoding::type ENCODING>
inline bool ParquetBoolDecoder::DecodeValue(bool* RESTRICT value) RESTRICT {
  DCHECK_EQ(ENCODING, encoding_);
  if (LIKELY(unpacked_value_idx_ < num_unpacked_values_)) {
    *value = unpacked_values_[unpacked_value_idx_++];
  } else {
    // Unpack as many values as we can into the buffer. We expect to read at least one
    // value.
    if (ENCODING == parquet::Encoding::PLAIN) {
      num_unpacked_values_ =
          bool_values_.UnpackBatch(1, UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
    } else {
      num_unpacked_values_ =
          rle_decoder_.GetValues(UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
    }

    if (UNLIKELY(num_unpacked_values_ == 0)) {
      return false;
    }
    *value = unpacked_values_[0];
    unpacked_value_idx_ = 1;
  }
  return true;
}
} // namespace impala
