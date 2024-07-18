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

#include "runtime/outbound-row-batch.h"
#include "runtime/outbound-row-batch.inline.h"
#include "util/compress.h"
#include "util/scope-exit-trigger.h"

namespace impala {

Status OutboundRowBatch::PrepareForSend(int num_tuples_per_row,
    TrackedString* compression_scratch, bool used_append_row) {
  if (used_append_row) {
    DCHECK_GE(tuple_data_.size(), tuple_data_offset_);
    tuple_data_.resize(tuple_data_offset_);
  } else {
    DCHECK_EQ(tuple_data_offset_, 0);
  }
  bool is_compressed = false;
  int64_t uncompressed_size = tuple_data_.size();
  if (uncompressed_size > 0 && compression_scratch != nullptr) {
    RETURN_IF_ERROR(TryCompress(compression_scratch, &is_compressed));
  }
  int num_tuples = tuple_offsets_.size();
  DCHECK_EQ(num_tuples % num_tuples_per_row, 0);
  int num_rows = num_tuples / num_tuples_per_row;
  SetHeader(num_rows, num_tuples_per_row, uncompressed_size, is_compressed);
  return Status::OK();
}

Status OutboundRowBatch::TryCompress(TrackedString* compression_scratch,
    bool* is_compressed) {
  DCHECK(compression_scratch != nullptr);
  Lz4Compressor compressor(nullptr, false);
  RETURN_IF_ERROR(compressor.Init());
  auto compressor_cleanup =
      MakeScopeExitTrigger([&compressor]() { compressor.Close(); });

  *is_compressed = false;
  int64_t uncompressed_size = tuple_data_.size();
  // If the input size is too large for LZ4 to compress, MaxOutputLen() will return 0.
  int64_t compressed_size = compressor.MaxOutputLen(uncompressed_size);
  if (compressed_size == 0) {
      return Status(TErrorCode::LZ4_COMPRESSION_INPUT_TOO_LARGE, uncompressed_size);
  }
  DCHECK_GT(compressed_size, 0);
  if (compression_scratch->size() < compressed_size) {
      compression_scratch->resize(compressed_size);
  }

  uint8_t* input = reinterpret_cast<uint8_t*>(tuple_data_.data());
  uint8_t* compressed_output = const_cast<uint8_t*>(
      reinterpret_cast<const uint8_t*>(compression_scratch->data()));
  RETURN_IF_ERROR(compressor.ProcessBlock(
      true, uncompressed_size, input, &compressed_size, &compressed_output));
  if (LIKELY(compressed_size < uncompressed_size)) {
      compression_scratch->resize(compressed_size);
      tuple_data_.swap(*compression_scratch);
      *is_compressed = true;
      // TODO: could copy to a smaller buffer if compressed data is much smaller to
      //       save memory
  }
  VLOG_ROW << "uncompressed size: " << uncompressed_size << ", compressed size: "
      << compressed_size;
  return Status::OK();
}

void OutboundRowBatch::SetHeader(int num_rows, int num_tuples_per_row,
    int64_t uncompressed_size, bool is_compressed) {
  header_.Clear();
  header_.set_num_rows(num_rows);
  header_.set_num_tuples_per_row(num_tuples_per_row);
  header_.set_uncompressed_size(uncompressed_size);
  header_.set_compression_type(
      is_compressed ? CompressionTypePB::LZ4 : CompressionTypePB::NONE);
}

void OutboundRowBatch::Reset() {
  header_.Clear();
  tuple_offsets_.clear();
  tuple_data_offset_ = 0;
  // Do not clear tuple_data_ to avoid unnecessary delete + allocate.
}

}
