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

#include "exec/parquet/parquet-delta-length-byte-array-decoder.h"

#include <algorithm>

#include <gutil/strings/substitute.h>

#include "common/names.h"

namespace impala {

Status ParquetDeltaLengthByteArrayDecoder::NewPage(
    const uint8_t* data, int data_len, int page_num_values) {
  DCHECK_GE(data_len, 0);
  DCHECK_GE(page_num_values, 0);
  initialized_ = false;
  has_only_small_ = true;
  total_value_count_ = 0;
  buf_pos_ = 0;
  buf_filled_ = 0;
  values_remaining_ = 0;
  data_ = nullptr;
  bytes_remaining_ = 0;

  if (data_len == 0) {
    // Empty page: zero values, nothing to do.
    initialized_ = true;
    return Status::OK();
  }

  // Phase 1: read the delta header to learn the value count, then skip all length
  // values to advance to the end of the lengths section. This reveals where the
  // concatenated string data begins without allocating O(num_values) memory.
  RETURN_IF_ERROR(len_decoder_.NewPage(data, data_len));
  const std::size_t raw_count = len_decoder_.GetTotalValueCount();

  // Sanity-check raw_count against page_num_values from the data page header
  // (which includes NULLs).
  if (UNLIKELY(raw_count > static_cast<std::size_t>(page_num_values))) {
    return Status(strings::Substitute(
        "DELTA_LENGTH_BYTE_ARRAY: encoded length count $0 exceeds "
        "page num_values $1.", raw_count, page_num_values));
  }
  // Safe to narrow: raw_count <= page_num_values which is int32.
  const int num_values = static_cast<int>(raw_count);

  // TODO: finding the data section only needs the block headers, but SkipValues
  // decodes every delta. Scanning block headers alone, and/or decoding all
  // lengths at once into a larger buffer, would avoid this extra pass.
  if (num_values > 0) {
    int skipped = len_decoder_.SkipValues(num_values);
    if (UNLIKELY(skipped != num_values)) {
      return Status(strings::Substitute(
          "DELTA_LENGTH_BYTE_ARRAY: failed to scan lengths section. "
          "Expected to skip $0 values, skipped $1.", num_values, skipped));
    }
  }

  const int data_section_bytes = len_decoder_.BytesLeftInPage();
  data_ = data + (data_len - data_section_bytes);
  bytes_remaining_ = data_section_bytes;
  total_value_count_ = num_values;
  values_remaining_ = num_values;

  // Phase 2: reset the decoder so that lengths are decoded lazily on demand.
  RETURN_IF_ERROR(len_decoder_.NewPage(data, data_len));

  initialized_ = true;
  return Status::OK();
}

int ParquetDeltaLengthByteArrayDecoder::FillLengthsBuffer() {
  DCHECK_EQ(buf_pos_, buf_filled_);
  if (values_remaining_ == 0) return 0;
  const int to_decode = std::min(values_remaining_, LENGTHS_BATCH_SIZE);
  int decoded = len_decoder_.NextValues(
      to_decode, reinterpret_cast<uint8_t*>(lengths_buf_), sizeof(int32_t));
  if (UNLIKELY(decoded != to_decode)) return -1;
  // Validate that the batch of lengths fits within the remaining string data.
  int64_t batch_bytes = 0;
  for (int i = 0; i < decoded; ++i) {
    if (UNLIKELY(lengths_buf_[i] < 0)) return -1;
    batch_bytes += lengths_buf_[i];
  }
  if (UNLIKELY(batch_bytes > bytes_remaining_)) {
    LOG(ERROR) << "DELTA_LENGTH_BYTE_ARRAY: page too small. "
               << "data section bytes=" << bytes_remaining_
               << ", required bytes=" << batch_bytes;
    return -1;
  }
  buf_pos_ = 0;
  buf_filled_ = to_decode;
  values_remaining_ -= to_decode;
  return to_decode;
}

int ParquetDeltaLengthByteArrayDecoder::NextValue(StringValue* out) {
  return NextValues(1, out, sizeof(StringValue));
}

int ParquetDeltaLengthByteArrayDecoder::NextValues(
    int num_values, StringValue* out, int64_t stride) {
  DCHECK(initialized_);
  DCHECK_GE(num_values, 0);

  const int available = (buf_filled_ - buf_pos_) + values_remaining_;
  if (UNLIKELY(available == 0)) return 0;
  const int to_read = std::min(num_values, available);

  uint8_t* out_bytes = reinterpret_cast<uint8_t*>(out);
  for (int i = 0; i < to_read; ++i) {
    if (UNLIKELY(buf_pos_ == buf_filled_)) {
      if (UNLIKELY(FillLengthsBuffer() <= 0)) return -1;
    }
    const int32_t len = lengths_buf_[buf_pos_++];
    StringValue* sv = reinterpret_cast<StringValue*>(out_bytes);
    *sv = StringValue(reinterpret_cast<char*>(const_cast<uint8_t*>(data_)), len);
    // Smallify() is safe here because the StringValue is in the output buffer
    // and exclusively owned by the decoder at this point. For strings that are
    // too long to inline, the StringValue keeps pointing into the page buffer,
    // so the caller must keep the page buffer alive (tracked via has_only_small_).
    if (!sv->Smallify()) {
      DCHECK(!sv->CanBeSmallified());
      has_only_small_ = false;
    }
    data_ += len;
    bytes_remaining_ -= len;
    out_bytes += stride;
  }
  return to_read;
}

int ParquetDeltaLengthByteArrayDecoder::SkipValues(int num_values) {
  DCHECK(initialized_);
  DCHECK_GE(num_values, 0);

  const int available = (buf_filled_ - buf_pos_) + values_remaining_;
  if (UNLIKELY(available == 0)) return 0;
  const int to_skip = std::min(num_values, available);

  int skipped = 0;
  while (skipped < to_skip) {
    if (UNLIKELY(buf_pos_ == buf_filled_)) {
      if (UNLIKELY(FillLengthsBuffer() <= 0)) return -1;
    }
    const int32_t len = lengths_buf_[buf_pos_++];
    data_ += len;
    bytes_remaining_ -= len;
    ++skipped;
  }
  return skipped;
}

} // namespace impala
