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

// TODO: this really needs codegen
inline bool BufferedTupleStream::DeepCopy(TupleRow* row) {
  if (UNLIKELY(write_block_ == NULL)) return false;
  DCHECK(write_block_->is_pinned());

  // Total bytes allocated in write_block_ for this row. Saved so we can roll
  // back if this row doesn't fit.
  int bytes_allocated = 0;

  // Copy the fixed len tuples.
  if (UNLIKELY(write_block_->BytesRemaining() < fixed_tuple_row_size_)) return false;
  uint8_t* tuple_buf = write_block_->Allocate<uint8_t>(fixed_tuple_row_size_);
  bytes_allocated += fixed_tuple_row_size_;
  for (int i = 0; i < desc_.tuple_descriptors().size(); ++i) {
    int tuple_size = desc_.tuple_descriptors()[i]->byte_size();
    Tuple* t = row->GetTuple(i);
    memcpy(tuple_buf, t, tuple_size);
    tuple_buf += tuple_size;
  }

  // Copy string slots. Note: we do not need to convert the string ptrs to offsets
  // on the write path, only on the read. The tuple data is immediately followed
  // by the string data so only the len information is necessary.
  for (int i = 0; i < string_slots_.size(); ++i) {
    Tuple* tuple = row->GetTuple(string_slots_[i].first);
    if (tuple == NULL) continue;
    for (int j = 0; j < string_slots_[i].second.size(); ++j) {
      const SlotDescriptor* slot_desc = string_slots_[i].second[j];
      if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
      StringValue* sv = tuple->GetStringSlot(slot_desc->tuple_offset());
      if (LIKELY(sv->len > 0)) {
        if (UNLIKELY(write_block_->BytesRemaining() < sv->len)) {
          write_block_->ReturnAllocation(bytes_allocated);
          return false;
        }
        uint8_t* buf = write_block_->Allocate<uint8_t>(sv->len);
        bytes_allocated += sv->len;
        memcpy(buf, sv->ptr, sv->len);
      }
    }
  }

  ++num_rows_;
  return true;
}

inline bool BufferedTupleStream::AddRow(TupleRow* row) {
  if (LIKELY(DeepCopy(row))) return true;
  bool got_block = false;
  status_ = NewBlockForWrite(&got_block);
  if (!status_.ok() || !got_block) return false;
  return DeepCopy(row);
}

inline uint8_t* BufferedTupleStream::AllocateRow(int size) {
  DCHECK(write_block_ != NULL);
  DCHECK(write_block_->is_pinned());
  DCHECK_GE(size, fixed_tuple_row_size_);
  if (UNLIKELY(write_block_->BytesRemaining() < size)) {
    bool got_block = false;
    status_ = NewBlockForWrite(&got_block);
    if (!status_.ok() || !got_block) return NULL;
  }
  DCHECK_GE(write_block_->BytesRemaining(), size);
  ++num_rows_;
  return write_block_->Allocate<uint8_t>(size);
}

}

#endif
