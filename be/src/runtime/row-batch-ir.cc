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

#include "runtime/row-batch.h"

#include "runtime/outbound-row-batch.h"
#include "util/fixed-size-hash-table.h"

namespace impala {

Status RowBatch::Serialize(
     OutboundRowBatch* output_batch, TrackedString* compression_scratch) {
  return Serialize(output_batch, UseFullDedup(), compression_scratch);
}

Status RowBatch::Serialize(
    OutboundRowBatch* output_batch, bool full_dedup, TrackedString* compression_scratch) {
  // As part of the serialization process we deduplicate tuples to avoid serializing a
  // Tuple multiple times for the RowBatch. By default we only detect duplicate tuples
  // in adjacent rows only. If full deduplication is enabled, we will build a
  // map to detect non-adjacent duplicates. Building this map comes with significant
  // overhead, so is only worthwhile in the uncommon case of many non-adjacent duplicates.
  RETURN_IF_ERROR(SerializeInternal(full_dedup, output_batch));
  RETURN_IF_ERROR(output_batch->PrepareForSend(row_desc_->tuple_descriptors().size(),
      compression_scratch, true));
  return Status::OK();
}

Status RowBatch::SerializeInternal(bool full_dedup, OutboundRowBatch* output_batch) {
  OutboundRowBatch::DedupMap distinct_tuples;
  OutboundRowBatch::DedupMap* distinct_tuples_ptr =
      full_dedup ? &distinct_tuples : nullptr;
  output_batch->Reset();
  if (full_dedup) {
    RETURN_IF_ERROR(distinct_tuples.Init(num_rows_ * num_tuples_per_row_ * 2, 0));
  }

  // Copy tuple data of unique tuples, including strings, into output_batch (converting
  // string pointers into offsets in the process).
  for (int i = 0; i < num_rows_; ++i) {
    const TupleRow* prev_row = LIKELY(i > 0) ? GetRow(i - 1) : nullptr;
    RETURN_IF_ERROR(output_batch->AppendRowWithDedup(
        GetRow(i), prev_row, distinct_tuples_ptr, row_desc_));
  }
  return Status::OK();
}

} // namespace impala