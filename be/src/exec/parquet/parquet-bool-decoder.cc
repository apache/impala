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

#include "exec/parquet/parquet-bool-decoder.h"

#include "util/mem-util.h"

#include "common/names.h"

namespace impala {

bool ParquetBoolDecoder::SetData(
    parquet::Encoding::type encoding, uint8_t* data, int size) {
  encoding_ = encoding;
  // Only the relevant decoder is initialized for a given data page.
  switch (encoding) {
    case parquet::Encoding::PLAIN:
      bool_values_.Reset(data, size);
      break;
    case parquet::Encoding::RLE:
      // The first 4 bytes contain the size of the encoded data. This information is
      // redundant, as this is the last part of the data page, and the number of
      // remaining bytes is already known.
      rle_decoder_.Reset(data + 4, size - 4, 1);
      break;
    default:
      return false;
  }
  num_unpacked_values_ = 0;
  unpacked_value_idx_ = 0;
  return true;
}

bool ParquetBoolDecoder::DecodeValues(
    int64_t stride, int64_t count, bool* RESTRICT first_value) RESTRICT {
  if (encoding_ == parquet::Encoding::PLAIN) {
    return DecodeValues<parquet::Encoding::PLAIN>(stride, count, first_value);
  } else {
    DCHECK_EQ(encoding_, parquet::Encoding::RLE);
    return DecodeValues<parquet::Encoding::RLE>(stride, count, first_value);
  }
}

template <parquet::Encoding::type ENCODING>
bool ParquetBoolDecoder::DecodeValues(
    int64_t stride, int64_t count, bool* RESTRICT first_value) RESTRICT {
  // TODO: we could optimise this further if needed by bypassing 'unpacked_values_'.
  StrideWriter<bool> out(first_value, stride);
  for (int64_t i = 0; i < count; ++i) {
    if (UNLIKELY(!DecodeValue<ENCODING>(out.Advance()))) return false;
  }
  return true;
}

bool ParquetBoolDecoder::SkipValues(int num_values) {
  DCHECK_GE(num_values, 0);
  int skip_cached = min(num_unpacked_values_ - unpacked_value_idx_, num_values);
  unpacked_value_idx_ += skip_cached;
  if (skip_cached == num_values) return true;
  int num_remaining = num_values - skip_cached;
  if (encoding_ == parquet::Encoding::PLAIN) {
    int num_to_skip = BitUtil::RoundDownToPowerOf2(num_remaining, 32);
    if (num_to_skip > 0) {
      bool skipped = bool_values_.SkipBatch(1, num_to_skip);
      DCHECK(skipped);
    }
    num_remaining -= num_to_skip;
    if (num_remaining > 0) {
      DCHECK_LE(num_remaining, UNPACKED_BUFFER_LEN);
      num_unpacked_values_ = bool_values_.UnpackBatch(1, UNPACKED_BUFFER_LEN,
          &unpacked_values_[0]);
      if (UNLIKELY(num_unpacked_values_ < num_remaining)) return false;
      unpacked_value_idx_ = num_remaining;
    }
    return true;
  } else {
    // rle_decoder_.SkipValues() might fill its internal buffer 'literal_buffer_'.
    // This can result in sub-optimal decoding later, because 'literal_buffer_' might
    // be used again and again, especially when reading a very long literal run.
    DCHECK_EQ(encoding_, parquet::Encoding::RLE);
    return rle_decoder_.SkipValues(num_remaining) == num_remaining;
  }
}

} // namespace impala
