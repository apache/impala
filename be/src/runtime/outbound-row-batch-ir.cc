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

#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"

namespace impala {

Status OutboundRowBatch::AppendRowWithDedup(
    const TupleRow* row, const TupleRow* prev_row, DedupMap* distinct_tuples,
    const RowDescriptor* row_desc) noexcept {
  DCHECK(row != nullptr);
  int num_tuples = row_desc->num_tuples_no_inline();
  vector<TupleDescriptor*>::const_iterator desc =
      row_desc->tuple_descriptors().begin();
  for (int tuple_idx = 0; tuple_idx < num_tuples; ++desc, ++tuple_idx) {
    RETURN_IF_ERROR(AppendTupleWithDedup(row, prev_row, tuple_idx, distinct_tuples, *desc,
        (*desc)->byte_size(), num_tuples));
  }
  return Status::OK();
}

Status OutboundRowBatch::AppendTupleWithDedup(const TupleRow* row,
    const TupleRow* prev_row, int tuple_idx, DedupMap* distinct_tuples,
    TupleDescriptor* desc, int byte_size, int num_tuples) {
  Tuple* tuple = row->GetTuple(tuple_idx);
  Tuple* prev_tuple = prev_row ? prev_row->GetTuple(tuple_idx) : nullptr;

  if (UNLIKELY(tuple == nullptr)) {
    // NULLs are encoded as -1
    tuple_offsets_.push_back(-1);
    return Status::OK();
  } else if (UNLIKELY(prev_tuple == tuple)) {
    // Fast tuple deduplication for adjacent rows.
    DCHECK_GT(tuple_offsets_.size(), 0);
    int prev_tuple_idx = tuple_offsets_.size() - num_tuples;
    DCHECK_GE(prev_tuple_idx, 0);
    tuple_offsets_.push_back(tuple_offsets_[prev_tuple_idx]);
    return Status::OK();
  } else if (UNLIKELY(distinct_tuples != nullptr)) {
    if (byte_size == 0) {
      // Zero-length tuples can be represented as nullptr.
      tuple_offsets_.push_back(-1);
      return Status::OK();
    }
    int* dedupd_offset = distinct_tuples->FindOrInsert(tuple, tuple_data_offset_);
    if (*dedupd_offset != tuple_data_offset_) {
      // Repeat of tuple
      DCHECK_GE(*dedupd_offset, 0);
      tuple_offsets_.push_back(*dedupd_offset);
      return Status::OK();
    }
  }

  // Record offset before creating copy (which increments offset and tuple_data)
  tuple_offsets_.push_back(tuple_data_offset_);
  RETURN_IF_ERROR(AppendTuple(tuple, desc));
  DCHECK_LE(tuple_data_offset_, tuple_data_.size());

  return Status::OK();
}

bool IR_ALWAYS_INLINE StatusOK(Status* status) {
  return status->ok();
}

} // namespace impala
