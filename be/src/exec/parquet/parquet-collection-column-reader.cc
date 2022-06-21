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

#include "parquet-collection-column-reader.h"

#include "runtime/collection-value-builder.h"

namespace impala {

bool CollectionColumnReader::NextLevels() {
  DCHECK(!children_.empty());
  DCHECK_LE(rep_level_, new_collection_rep_level());
  for (int c = 0; c < children_.size(); ++c) {
    do {
      // TODO: verify somewhere that all column readers are at end
      if (!children_[c]->NextLevels()) return false;
    } while (children_[c]->rep_level() > new_collection_rep_level());
  }
  UpdateDerivedState();
  return true;
}

bool CollectionColumnReader::ReadValue(MemPool* pool, Tuple* tuple) {
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor())
      << "Caller should have called NextLevels() until we are ready to read a value";

  if (tuple_offset_ == -1) {
    return CollectionColumnReader::NextLevels();
  } else if (def_level_ >= max_def_level()) {
    return ReadSlot(tuple->GetCollectionSlot(tuple_offset_), pool);
  } else {
    // Collections add an extra def level, so it is possible to distinguish between
    // NULL and empty collections. See hdfs-parquet-scanner.h for more detailed
    // explanation.
    if (def_level_ == max_def_level() - 1) {
      CollectionValue* slot = tuple->GetCollectionSlot(tuple_offset_);
      *slot = CollectionValue();
    } else {
      SetNullSlot(tuple);
    }
    return CollectionColumnReader::NextLevels();
  }
}

bool CollectionColumnReader::ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) {
  return CollectionColumnReader::ReadValue(pool, tuple);
}

bool CollectionColumnReader::ReadValueBatch(MemPool* pool, int max_values,
    int tuple_size, uint8_t* tuple_mem, int* num_values) {
  // The below loop requires that NextLevels() was called previously to populate
  // 'def_level_' and 'rep_level_'. Ensure it is called at the start of each
  // row group.
  if (def_level_ == ParquetLevel::INVALID_LEVEL && !NextLevels()) return false;

  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem + val_count * tuple_size);
    if (def_level_ < def_level_of_immediate_repeated_ancestor()) {
      // A containing repeated field is empty or NULL
      continue_execution = NextLevels();
      continue;
    }
    // Fill in position slots if applicable
    if (pos_slot_desc() != nullptr) {
      ReadItemPositionNonBatched(tuple->GetBigIntSlot(pos_slot_desc()->tuple_offset()));
    } else if (file_pos_slot_desc() != nullptr) {
      ReadFilePositionNonBatched(
          tuple->GetBigIntSlot(file_pos_slot_desc()->tuple_offset()));
    }
    continue_execution = ReadValue(pool, tuple);
    ++val_count;
    if (SHOULD_TRIGGER_COL_READER_DEBUG_ACTION(val_count)) {
      continue_execution &= ColReaderDebugAction(&val_count);
    }
  }
  *num_values = val_count;
  return continue_execution;
}

bool CollectionColumnReader::ReadNonRepeatedValueBatch(MemPool* pool,
    int max_values, int tuple_size, uint8_t* tuple_mem, int* num_values) {
  // The below loop requires that NextLevels() was called previously to populate
  // 'def_level_' and 'rep_level_'. Ensure it is called at the start of each
  // row group.
  if (def_level_ == ParquetLevel::INVALID_LEVEL && !NextLevels()) return false;

  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem + val_count * tuple_size);
    continue_execution = ReadNonRepeatedValue(pool, tuple);
    ++val_count;
    if (SHOULD_TRIGGER_COL_READER_DEBUG_ACTION(val_count)) {
      continue_execution &= ColReaderDebugAction(&val_count);
    }
  }
  *num_values = val_count;
  return continue_execution;
}

bool CollectionColumnReader::ReadSlot(CollectionValue* slot, MemPool* pool) {
  DCHECK(!children_.empty());
  DCHECK_LE(rep_level_, new_collection_rep_level());

  // Recursively read the collection into a new CollectionValue.
  *slot = CollectionValue();
  CollectionValueBuilder builder(
      slot, *slot_desc_->children_tuple_descriptor(), pool, parent_->state_);
  bool continue_execution =
      parent_->AssembleCollection(children_, new_collection_rep_level(), &builder);
  if (!continue_execution) return false;

  // AssembleCollection() advances child readers, so we don't need to call NextLevels()
  UpdateDerivedState();
  return true;
}

void CollectionColumnReader::UpdateDerivedState() {
  // We don't need to cap our def_level_ at max_def_level(). We always check def_level_
  // >= max_def_level() to check if the collection is defined.
  // TODO: consider capping def_level_ at max_def_level()
  def_level_ = children_[0]->def_level();
  rep_level_ = children_[0]->rep_level();

  // All children should have been advanced to the beginning of the next collection
  for (int i = 0; i < children_.size(); ++i) {
    DCHECK_EQ(children_[i]->rep_level(), rep_level_);
    if (def_level_ < max_def_level()) {
      // Collection not defined
      FILE_CHECK_EQ(children_[i]->def_level(), def_level_);
    } else {
      // Collection is defined
      FILE_CHECK_GE(children_[i]->def_level(), max_def_level());
    }
  }

  if (RowGroupAtEnd()) {
    // No more values
    pos_current_value_ = ParquetLevel::INVALID_POS;
  } else if (rep_level_ <= max_rep_level() - 2) {
    // Reset position counter if we are at the start of a new parent collection (i.e.,
    // the current collection is the first item in a new parent collection).
    pos_current_value_ = 0;
  }
}

bool CollectionColumnReader::SkipRows(int64_t num_rows, int64_t skip_row_id) {
  DCHECK(!children_.empty());
  for (int c = 0; c < children_.size(); ++c) {
    if (!children_[c]->SkipRows(num_rows, skip_row_id)) return false;
  }
  UpdateDerivedState();
  return true;
}
} // namespace impala
