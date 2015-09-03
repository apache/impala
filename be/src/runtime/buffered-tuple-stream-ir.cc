// Copyright 2014 Cloudera Inc.
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

#include "runtime/buffered-tuple-stream.inline.h"

#include "runtime/array-value.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"

using namespace impala;

bool BufferedTupleStream::DeepCopy(TupleRow* row, uint8_t** dst) {
  if (nullable_tuple_) {
    return DeepCopyInternal<true>(row, dst);
  } else {
    return DeepCopyInternal<false>(row, dst);
  }
}

// TODO: this really needs codegen
// TODO: in case of duplicate tuples, this can redundantly serialize data.
template <bool HasNullableTuple>
bool BufferedTupleStream::DeepCopyInternal(TupleRow* row, uint8_t** dst) {
  if (UNLIKELY(write_block_ == NULL)) return false;
  DCHECK_GE(null_indicators_write_block_, 0);
  DCHECK(write_block_->is_pinned()) << DebugString() << std::endl
      << write_block_->DebugString();

  const uint64_t tuples_per_row = desc_.tuple_descriptors().size();
  if (UNLIKELY((write_block_->BytesRemaining() < fixed_tuple_row_size_) ||
              (HasNullableTuple &&
              (write_tuple_idx_ + tuples_per_row > null_indicators_write_block_ * 8)))) {
    return false;
  }
  // Allocate the maximum possible buffer for the fixed portion of the tuple.
  uint8_t* tuple_buf = write_block_->Allocate<uint8_t>(fixed_tuple_row_size_);
  if (dst != NULL) *dst = tuple_buf;
  // Total bytes allocated in write_block_ for this row. Saved so we can roll back
  // if this row doesn't fit.
  int bytes_allocated = fixed_tuple_row_size_;

  // Copy the not NULL fixed len tuples. For the NULL tuples just update the NULL tuple
  // indicator.
  if (HasNullableTuple) {
    DCHECK_GT(null_indicators_write_block_, 0);
    uint8_t* null_word = NULL;
    uint32_t null_pos = 0;
    // Calculate how much space it should return.
    int to_return = 0;
    for (int i = 0; i < tuples_per_row; ++i) {
      null_word = write_block_->buffer() + (write_tuple_idx_ >> 3); // / 8
      null_pos = write_tuple_idx_ & 7;
      ++write_tuple_idx_;
      const int tuple_size = desc_.tuple_descriptors()[i]->byte_size();
      Tuple* t = row->GetTuple(i);
      const uint8_t mask = 1 << (7 - null_pos);
      if (t != NULL) {
        *null_word &= ~mask;
        memcpy(tuple_buf, t, tuple_size);
        tuple_buf += tuple_size;
      } else {
        *null_word |= mask;
        to_return += tuple_size;
      }
    }
    DCHECK_LE(write_tuple_idx_ - 1, null_indicators_write_block_ * 8);
    write_block_->ReturnAllocation(to_return);
    bytes_allocated -= to_return;
  } else {
    // If we know that there are no nullable tuples no need to set the nullability flags.
    DCHECK_EQ(null_indicators_write_block_, 0);
    for (int i = 0; i < tuples_per_row; ++i) {
      const int tuple_size = desc_.tuple_descriptors()[i]->byte_size();
      Tuple* t = row->GetTuple(i);
      // TODO: Once IMPALA-1306 (Avoid passing empty tuples of non-materialized slots)
      // is delivered, the check below should become DCHECK(t != NULL).
      DCHECK(t != NULL || tuple_size == 0);
      memcpy(tuple_buf, t, tuple_size);
      tuple_buf += tuple_size;
    }
  }

  // Copy string slots. Note: we do not need to convert the string ptrs to offsets
  // on the write path, only on the read. The tuple data is immediately followed
  // by the string data so only the len information is necessary.
  for (int i = 0; i < string_slots_.size(); ++i) {
    Tuple* tuple = row->GetTuple(string_slots_[i].first);
    if (HasNullableTuple && tuple == NULL) continue;
    if (UNLIKELY(!CopyStrings(tuple, string_slots_[i].second, &bytes_allocated))) {
      write_block_->ReturnAllocation(bytes_allocated);
      return false;
    }
  }

  // Copy collection slots. We copy array data in a well-defined order so we do not need
  // to convert pointers to offsets on the write path.
  for (int i = 0; i < collection_slots_.size(); ++i) {
    Tuple* tuple = row->GetTuple(collection_slots_[i].first);
    if (HasNullableTuple && tuple == NULL) continue;
    if (UNLIKELY(!CopyCollections(tuple, collection_slots_[i].second,
        &bytes_allocated))) {
      write_block_->ReturnAllocation(bytes_allocated);
      return false;
    }
  }

  write_block_->AddRow();
  ++num_rows_;
  return true;
}

bool BufferedTupleStream::CopyStrings(const Tuple* tuple,
    const vector<SlotDescriptor*>& string_slots, int* bytes_allocated) {
  for (int i = 0; i < string_slots.size(); ++i) {
    const SlotDescriptor* slot_desc = string_slots[i];
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    const StringValue* sv = tuple->GetStringSlot(slot_desc->tuple_offset());
    if (LIKELY(sv->len > 0)) {
      if (UNLIKELY(write_block_->BytesRemaining() < sv->len)) {
        return false;
      }
      uint8_t* buf = write_block_->Allocate<uint8_t>(sv->len);
      (*bytes_allocated) += sv->len;
      memcpy(buf, sv->ptr, sv->len);
    }
  }
  return true;
}

bool BufferedTupleStream::CopyCollections(const Tuple* tuple,
    const vector<SlotDescriptor*>& collection_slots, int* bytes_allocated) {
  for (int i = 0; i < collection_slots.size(); ++i) {
    const SlotDescriptor* slot_desc = collection_slots[i];
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    const ArrayValue* av = tuple->GetCollectionSlot(slot_desc->tuple_offset());
    const TupleDescriptor& item_desc = *slot_desc->collection_item_descriptor();
    if (LIKELY(av->num_tuples > 0)) {
      int array_byte_size = av->num_tuples * item_desc.byte_size();
      if (UNLIKELY(write_block_->BytesRemaining() < array_byte_size)) {
        return false;
      }
      uint8_t* array_data = write_block_->Allocate<uint8_t>(array_byte_size);
      (*bytes_allocated) += array_byte_size;
      memcpy(array_data, av->ptr, array_byte_size);
      if (!item_desc.HasVarlenSlots()) continue;
      // Copy variable length data when present in array items.
      for (int j = 0; j < av->num_tuples; ++j) {
        Tuple* item = reinterpret_cast<Tuple*>(array_data);
        if (UNLIKELY(!CopyStrings(item, item_desc.string_slots(), bytes_allocated))) {
          return false;
        }
        if (UNLIKELY(!CopyCollections(item, item_desc.collection_slots(),
            bytes_allocated))) {
          return false;
        }
        array_data += item_desc.byte_size();
      }
    }
  }
  return true;
}
