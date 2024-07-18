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

#include "runtime/outbound-row-batch.h"

#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace impala {

Status OutboundRowBatch::AppendRow(const TupleRow* row, const RowDescriptor* row_desc) {
  DCHECK(row != nullptr);
  int num_tuples = row_desc->num_tuples_no_inline();
  vector<TupleDescriptor*>::const_iterator desc =
      row_desc->tuple_descriptors().begin();
  for (int j = 0; j < num_tuples; ++desc, ++j) {
    Tuple* tuple = row->GetTuple(j);
    if (UNLIKELY(tuple == nullptr)) {
      // NULLs are encoded as -1
      tuple_offsets_.push_back(-1);
      continue;
    }
    // Record offset before creating copy (which increments offset and tuple_data)
    tuple_offsets_.push_back(tuple_data_offset_);
    // Try appending tuple to current tuple_data_. If it doesn't fit to the buffer,
    // get the exact size needed for the tuple and allocate enough memory for it.
    // This allows iterating through the varlen slots of most tuples only once.
    if (UNLIKELY(!TryAppendTuple(tuple, *desc))) {
      int64_t tuple_size = tuple->TotalByteSize(**desc, true /*assume_smallify*/);
      int64_t new_size = tuple_data_offset_ + tuple_size;
      if (new_size > numeric_limits<int32_t>::max()) {
        return Status(
            TErrorCode::ROW_BATCH_TOO_LARGE, new_size, numeric_limits<int32_t>::max());
      }
      // TODO: Based on experience the below logic doubles the buffer size instead of
      // resizing to the exact size, similarly to vector. It would be clearer to use a
      // vector instead of string for tuple_data_, but in the long term it would be
      // better to use a fixed sized buffer (data_stream_sender_buffer_size) once var
      // len data is properly accounted for (see IMPALA-12594 for details).
      tuple_data_.resize(new_size);
      tuple_data_.resize(tuple_data_.capacity());
      DCHECK_GT(tuple_data_.size(), 0);
      bool retry_successful = TryAppendTuple(tuple, *desc);
      // As the buffer was resized based on the exact size of the tuple the second
      // attempt must succeed.
      DCHECK(retry_successful);
    }
    DCHECK_LE(tuple_data_offset_, tuple_data_.size());
  }
  return Status::OK();
}

bool OutboundRowBatch::TryAppendTuple(const Tuple* tuple, const TupleDescriptor* desc) {
  DCHECK(tuple != nullptr);
  DCHECK(desc != nullptr);
  if (tuple_data_.size() == 0) return false;
  DCHECK_GT(tuple_data_.size(), 0);
  uint8_t* dst = reinterpret_cast<uint8_t*>(&tuple_data_[0]) + tuple_data_offset_;
  uint8_t* dst_end = reinterpret_cast<uint8_t*>(&tuple_data_.back()) + 1;
  return tuple->TryDeepCopy(
      &dst, dst_end, &tuple_data_offset_, *desc, /* convert_ptrs */ true);
}

bool OutboundRowBatch::ReachedSizeLimit() {
    return RowBatch::AT_CAPACITY_MEM_USAGE <= tuple_data_offset_;
}

}
