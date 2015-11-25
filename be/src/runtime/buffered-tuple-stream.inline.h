// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef IMPALA_RUNTIME_TUPLE_BUFFERED_STREAM_INLINE_H
#define IMPALA_RUNTIME_TUPLE_BUFFERED_STREAM_INLINE_H

#include "runtime/buffered-tuple-stream.h"

#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"

namespace impala {

inline bool BufferedTupleStream::AddRow(TupleRow* row, Status* status) {
  DCHECK(!closed_);
  if (LIKELY(DeepCopy(row))) return true;
  bool got_block;
  int64_t row_size = ComputeRowSize(row);
  *status = NewBlockForWrite(row_size, &got_block);
  if (!status->ok() || !got_block) return false;
  return DeepCopy(row);
}

inline uint8_t* BufferedTupleStream::AllocateRow(int size, Status *status) {
  DCHECK(!closed_);
  if (UNLIKELY(write_block_ == NULL || write_block_->BytesRemaining() < size)) {
    bool got_block;
    *status = NewBlockForWrite(size, &got_block);
    if (!status->ok() || !got_block) return NULL;
  }
  DCHECK(write_block_ != NULL);
  DCHECK(write_block_->is_pinned());
  DCHECK_GE(write_block_->BytesRemaining(), size);
  ++num_rows_;
  write_block_->AddRow();
  return write_block_->Allocate<uint8_t>(size);
}

inline void BufferedTupleStream::GetTupleRow(const RowIdx& idx, TupleRow* row) const {
  DCHECK(row != NULL);
  DCHECK(!closed_);
  DCHECK(is_pinned());
  DCHECK(!delete_on_read_);
  DCHECK_EQ(blocks_.size(), block_start_idx_.size());
  DCHECK_LT(idx.block(), blocks_.size());

  uint8_t* data = block_start_idx_[idx.block()] + idx.offset();
  if (nullable_tuple_) {
    // Stitch together the tuples from the block and the NULL ones.
    const int tuples_per_row = desc_.tuple_descriptors().size();
    uint32_t tuple_idx = idx.idx() * tuples_per_row;
    for (int i = 0; i < tuples_per_row; ++i) {
      const uint8_t* null_word = block_start_idx_[idx.block()] + (tuple_idx >> 3);
      const uint32_t null_pos = tuple_idx & 7;
      const bool is_not_null = ((*null_word & (1 << (7 - null_pos))) == 0);
      row->SetTuple(i, reinterpret_cast<Tuple*>(
          reinterpret_cast<uint64_t>(data) * is_not_null));
      data += desc_.tuple_descriptors()[i]->byte_size() * is_not_null;
      ++tuple_idx;
    }
  } else {
    for (int i = 0; i < desc_.tuple_descriptors().size(); ++i) {
      row->SetTuple(i, reinterpret_cast<Tuple*>(data));
      data += desc_.tuple_descriptors()[i]->byte_size();
    }
  }
}

}

#endif
