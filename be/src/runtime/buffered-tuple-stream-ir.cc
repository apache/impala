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

#include "runtime/buffered-tuple-stream.inline.h"

#include "runtime/string-value.h"

using namespace impala;

bool BufferedTupleStream::CopyTupleNullIndicators(TupleRow* row, int num_tuples,
    uint8_t** data, const uint8_t* data_end) {
  int null_indicator_bytes = BitUtil::RoundUpNumBytes(num_tuples);
  if (UNLIKELY(*data + null_indicator_bytes > data_end)) return false;

  uint8_t* null_indicators = *data;
  *data += null_indicator_bytes;
  memset(null_indicators, 0, null_indicator_bytes);

  for (int tuple_idx = 0; tuple_idx < num_tuples; ++tuple_idx) {
    Tuple* t = row->GetTuple(tuple_idx);
    uint8_t* null_word = null_indicators + (tuple_idx >> 3);
    const uint32_t null_pos = tuple_idx & 7;
    const uint8_t mask = 1 << (7 - null_pos);
    if (t == nullptr) {
      *null_word |= mask;
    }
  }

  return true;
}

bool BufferedTupleStream::CopyTupleFixedLen(TupleRow* row, int tuple_idx,
    int tuple_size, uint8_t** data, const uint8_t* data_end, bool check_null_tuple) {
  Tuple* tuple = row->GetTuple(tuple_idx);
  if (check_null_tuple && tuple == nullptr) return true;
  if (UNLIKELY(*data + tuple_size > data_end)) return false;

  memcpy(*data, tuple, tuple_size);
  *data += tuple_size;

  return true;
}

bool BufferedTupleStream::CopyString(const Tuple *tuple, int null_byte_offset,
    uint8_t null_bit_mask, int tuple_offset, uint8_t** data, const uint8_t* data_end) {
  NullIndicatorOffset null_indicator_offset;
  null_indicator_offset.bit_mask = null_bit_mask;
  null_indicator_offset.byte_offset = null_byte_offset;
  if (tuple->IsNull(null_indicator_offset)) return true;
  const StringValue* sv = tuple->GetStringSlot(tuple_offset);
  if (LIKELY(sv->Len() > 0) && !sv->IsSmall()) {
    StringValue::SimpleString s = sv->ToSimpleString();
    if (UNLIKELY(*data + s.len > data_end)) return false;

    memcpy(*data, s.ptr, s.len);
    *data += s.len;
  }
  return true;
}

template <bool HAS_NULLABLE_TUPLE>
bool BufferedTupleStream::DeepCopyCollections(TupleRow* row, uint8_t** data,
  const uint8_t* data_end) noexcept {
  // Copy inlined collection slots. We copy collection data in a well-defined order so
  // we do not need to convert pointers to offsets on the write path.
  for (int i = 0; i < inlined_coll_slots_.size(); ++i) {
    const Tuple* tuple = row->GetTuple(inlined_coll_slots_[i].first);
    if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
    if (UNLIKELY(!CopyCollections(tuple, inlined_coll_slots_[i].second, data,
        data_end))) {
      return false;
    }
  }

  return true;
}

bool BufferedTupleStream::AddRow(TupleRow* row, Status* status) noexcept {
  DCHECK(!closed_);
  DCHECK(has_write_iterator());
  if (UNLIKELY(write_page_ == nullptr || !DeepCopy(row, &write_ptr_, write_end_ptr_))) {
    return AddRowSlow(row, status);
  }
  DCHECK_LT(num_rows_, INT64_MAX);
  DCHECK_LT(write_page_->num_rows, INT64_MAX);
  ++num_rows_;
  ++write_page_->num_rows;
  return true;
}

// Instantiate required templates.
template bool BufferedTupleStream::DeepCopyCollections<true>
    (TupleRow* row, uint8_t** data, const uint8_t* data_end) noexcept;
template bool BufferedTupleStream::DeepCopyCollections<false>
    (TupleRow* row, uint8_t** data, const uint8_t* data_end) noexcept;
