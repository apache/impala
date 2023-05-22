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

#include "parquet-struct-column-reader.h"

namespace impala {

bool StructColumnReader::NextLevels() {
  DCHECK(!children_.empty());
  bool result = true;
  for (ParquetColumnReader* child_reader : children_) {
    result &= child_reader->NextLevels();
  }
  def_level_ = children_[0]->def_level();
  rep_level_ = children_[0]->rep_level();
  if (rep_level_ <= max_rep_level() - 1) pos_current_value_ = 0;
  return result;
}

template <bool IN_COLLECTION>
bool StructColumnReader::ReadValue(MemPool* pool, Tuple* tuple, bool* read_row) {
  DCHECK(!children_.empty());
  DCHECK(!*read_row);
  bool should_abort = true;
  if (def_level_ >= max_def_level()) {
    for (ParquetColumnReader* child_col_reader : children_) {
      if (IN_COLLECTION) {
        should_abort &= child_col_reader->ReadValue(pool, tuple);
      } else {
        should_abort &= child_col_reader->ReadNonRepeatedValue(pool, tuple);
      }
    }
    *read_row = true;
  } else {
    if (!HasNullCollectionAncestor<IN_COLLECTION>()) {
      SetNullSlot(tuple);
      *read_row = true;
    }
    should_abort = NextLevels();
  }

  def_level_ = children_[0]->def_level();
  rep_level_ = children_[0]->rep_level();
  if (rep_level_ <= max_rep_level() - 1) pos_current_value_ = 0;
  return should_abort;
}

template <bool IN_COLLECTION>
bool StructColumnReader::HasNullCollectionAncestor() const {
  if (!IN_COLLECTION) return false;
  // If none of the parents are NULL
  if (def_level_ >= max_def_level() - 1) return false;
  // There is a null ancestor. Have to check if there is a null collection
  // in the chain between this column reader and the topmost null ancestor.
  if (def_level_ < def_level_of_immediate_repeated_ancestor()) return true;
  return false;
}

bool StructColumnReader::ReadValue(MemPool* pool, Tuple* tuple) {
  bool dummy = false;
  return ReadValue<true>(pool, tuple, &dummy);
}

bool StructColumnReader::ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) {
  bool dummy = false;
  return ReadValue<false>(pool, tuple, &dummy);
}

bool StructColumnReader::ReadValueBatch(MemPool* pool, int max_values,
    int tuple_size, uint8_t* tuple_mem, int* num_values) {
  return ReadValueBatch<true>(pool, max_values, tuple_size, tuple_mem, num_values);
}

bool StructColumnReader::ReadNonRepeatedValueBatch(MemPool* pool, int max_values,
    int tuple_size, uint8_t* tuple_mem, int* num_values) {
  return ReadValueBatch<false>(pool, max_values, tuple_size, tuple_mem, num_values);
}

template <bool IN_COLLECTION>
bool StructColumnReader::ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
    uint8_t* RESTRICT tuple_mem, int* RESTRICT num_values) RESTRICT {
  if (def_level_ == ParquetLevel::INVALID_LEVEL && !NextLevels()) return false;

  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem + val_count * tuple_size);
    bool read_row = false;
    // Fill in position slots if applicable
    if (pos_slot_desc() != nullptr) {
      DCHECK(file_pos_slot_desc() == nullptr);
      ReadItemPositionBatched(rep_level_,
          tuple->GetBigIntSlot(pos_slot_desc()->tuple_offset()));
    } else if (file_pos_slot_desc() != nullptr) {
      DCHECK(pos_slot_desc() == nullptr);
      // It is OK to call the non-batched version because we let the child readers
      // determine the LastProcessedRow() and we use the non-bached ReadValue() functions
      // of the children.
      ReadFilePositionNonBatched(
          tuple->GetBigIntSlot(file_pos_slot_desc()->tuple_offset()));
    }
    continue_execution = ReadValue<IN_COLLECTION>(pool, tuple, &read_row);
    if (read_row) ++val_count;
    if (SHOULD_TRIGGER_COL_READER_DEBUG_ACTION(val_count)) {
      continue_execution &= ColReaderDebugAction(&val_count);
    }
  }
  *num_values = val_count;
  return continue_execution;
}

bool StructColumnReader::SkipRows(int64_t num_rows, int64_t skip_row_id) {
  // Structs are excluded from late materialization so no need to implement SkipRows().
  DCHECK(false);
  return true;
}

} // namespace impala
