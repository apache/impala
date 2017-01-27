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

#ifndef IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_V2_INLINE_H
#define IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_V2_INLINE_H

#include "runtime/buffered-tuple-stream-v2.h"

#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"
#include "util/bit-util.h"

namespace impala {

inline int BufferedTupleStreamV2::NullIndicatorBytesPerRow() const {
  DCHECK(has_nullable_tuple_);
  return BitUtil::RoundUpNumBytes(fixed_tuple_sizes_.size());
}

inline bool BufferedTupleStreamV2::AddRow(TupleRow* row, Status* status) noexcept {
  DCHECK(!closed_);
  if (LIKELY(DeepCopy(row))) return true;
  return AddRowSlow(row, status);
}

inline uint8_t* BufferedTupleStreamV2::AllocateRow(
    int fixed_size, int varlen_size, uint8_t** varlen_data, Status* status) {
  DCHECK(!closed_);
  DCHECK(!has_nullable_tuple_) << "AllocateRow does not support nullable tuples";
  const int total_size = fixed_size + varlen_size;
  if (UNLIKELY(write_page_ == nullptr || write_ptr_ + total_size > write_end_ptr_)) {
    return AllocateRowSlow(fixed_size, varlen_size, varlen_data, status);
  }
  DCHECK(write_page_ != nullptr);
  DCHECK(write_page_->is_pinned());
  DCHECK_LE(write_ptr_ + total_size, write_end_ptr_);
  ++num_rows_;
  ++write_page_->num_rows;

  uint8_t* fixed_data = write_ptr_;
  write_ptr_ += fixed_size;
  *varlen_data = write_ptr_;
  write_ptr_ += varlen_size;
  return fixed_data;
}
}

#endif
