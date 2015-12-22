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

#include "runtime/collection-value.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"

using namespace impala;

bool BufferedTupleStream::DeepCopy(TupleRow* row) {
  if (has_nullable_tuple_) {
    return DeepCopyInternal<true>(row);
  } else {
    return DeepCopyInternal<false>(row);
  }
}

// TODO: this really needs codegen
// TODO: in case of duplicate tuples, this can redundantly serialize data.
template <bool HasNullableTuple>
bool BufferedTupleStream::DeepCopyInternal(TupleRow* row) {
  if (UNLIKELY(write_block_ == NULL)) return false;
  DCHECK_GE(write_block_null_indicators_size_, 0);
  DCHECK(write_block_->is_pinned()) << DebugString() << std::endl
      << write_block_->DebugString();

  const uint64_t tuples_per_row = desc_.tuple_descriptors().size();
  uint32_t bytes_remaining = write_block_bytes_remaining();
  if (UNLIKELY((bytes_remaining < fixed_tuple_row_size_) ||
              (HasNullableTuple &&
              (write_tuple_idx_ + tuples_per_row > write_block_null_indicators_size_ * 8)))) {
    return false;
  }

  // Copy the not NULL fixed len tuples. For the NULL tuples just update the NULL tuple
  // indicator.
  if (HasNullableTuple) {
    DCHECK_GT(write_block_null_indicators_size_, 0);
    uint8_t* null_word = NULL;
    uint32_t null_pos = 0;
    for (int i = 0; i < tuples_per_row; ++i) {
      null_word = write_block_->buffer() + (write_tuple_idx_ >> 3); // / 8
      null_pos = write_tuple_idx_ & 7;
      ++write_tuple_idx_;
      const int tuple_size = fixed_tuple_sizes_[i];
      Tuple* t = row->GetTuple(i);
      const uint8_t mask = 1 << (7 - null_pos);
      if (t != NULL) {
        *null_word &= ~mask;
        memcpy(write_ptr_, t, tuple_size);
        write_ptr_ += tuple_size;
      } else {
        *null_word |= mask;
      }
    }
    DCHECK_LE(write_tuple_idx_ - 1, write_block_null_indicators_size_ * 8);
  } else {
    // If we know that there are no nullable tuples no need to set the nullability flags.
    DCHECK_EQ(write_block_null_indicators_size_, 0);
    for (int i = 0; i < tuples_per_row; ++i) {
      const int tuple_size = fixed_tuple_sizes_[i];
      Tuple* t = row->GetTuple(i);
      // TODO: Once IMPALA-1306 (Avoid passing empty tuples of non-materialized slots)
      // is delivered, the check below should become DCHECK(t != NULL).
      DCHECK(t != NULL || tuple_size == 0);
      memcpy(write_ptr_, t, tuple_size);
      write_ptr_ += tuple_size;
    }
  }

  // Copy inlined string slots. Note: we do not need to convert the string ptrs to offsets
  // on the write path, only on the read. The tuple data is immediately followed
  // by the string data so only the len information is necessary.
  for (int i = 0; i < inlined_string_slots_.size(); ++i) {
    const Tuple* tuple = row->GetTuple(inlined_string_slots_[i].first);
    if (HasNullableTuple && tuple == NULL) continue;
    if (UNLIKELY(!CopyStrings(tuple, inlined_string_slots_[i].second))) return false;
  }

  // Copy inlined collection slots. We copy collection data in a well-defined order so
  // we do not need to convert pointers to offsets on the write path.
  for (int i = 0; i < inlined_coll_slots_.size(); ++i) {
    const Tuple* tuple = row->GetTuple(inlined_coll_slots_[i].first);
    if (HasNullableTuple && tuple == NULL) continue;
    if (UNLIKELY(!CopyCollections(tuple, inlined_coll_slots_[i].second))) return false;
  }

  write_block_->AddRow();
  ++num_rows_;
  return true;
}

bool BufferedTupleStream::CopyStrings(const Tuple* tuple,
    const vector<SlotDescriptor*>& string_slots) {
  for (int i = 0; i < string_slots.size(); ++i) {
    const SlotDescriptor* slot_desc = string_slots[i];
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    const StringValue* sv = tuple->GetStringSlot(slot_desc->tuple_offset());
    if (LIKELY(sv->len > 0)) {
      if (UNLIKELY(write_block_bytes_remaining() < sv->len)) return false;

      memcpy(write_ptr_, sv->ptr, sv->len);
      write_ptr_ += sv->len;
    }
  }
  return true;
}

bool BufferedTupleStream::CopyCollections(const Tuple* tuple,
    const vector<SlotDescriptor*>& collection_slots) {
  for (int i = 0; i < collection_slots.size(); ++i) {
    const SlotDescriptor* slot_desc = collection_slots[i];
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    const CollectionValue* cv = tuple->GetCollectionSlot(slot_desc->tuple_offset());
    const TupleDescriptor& item_desc = *slot_desc->collection_item_descriptor();
    if (LIKELY(cv->num_tuples > 0)) {
      int coll_byte_size = cv->num_tuples * item_desc.byte_size();
      if (UNLIKELY(write_block_bytes_remaining() < coll_byte_size)) return false;
      uint8_t* coll_data = write_ptr_;
      memcpy(coll_data, cv->ptr, coll_byte_size);
      write_ptr_ += coll_byte_size;

      if (!item_desc.HasVarlenSlots()) continue;
      // Copy variable length data when present in collection items.
      for (int j = 0; j < cv->num_tuples; ++j) {
        const Tuple* item = reinterpret_cast<Tuple*>(coll_data);
        if (UNLIKELY(!CopyStrings(item, item_desc.string_slots()))) return false;
        if (UNLIKELY(!CopyCollections(item, item_desc.collection_slots()))) return false;
        coll_data += item_desc.byte_size();
      }
    }
  }
  return true;
}
