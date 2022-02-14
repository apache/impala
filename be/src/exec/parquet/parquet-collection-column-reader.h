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

#include "exec/parquet/parquet-complex-column-reader.h"

namespace impala {

/// Collections are not materialized directly in parquet files; only scalar values appear
/// in the file. CollectionColumnReader uses the definition and repetition levels of child
/// column readers to figure out the boundaries of each collection in this column.
class CollectionColumnReader : public ComplexColumnReader {
 public:
  CollectionColumnReader(
      HdfsParquetScanner* parent, const SchemaNode& node, const SlotDescriptor* slot_desc)
    : ComplexColumnReader(parent, node, slot_desc) {
    DCHECK(node_.is_repeated());
    if (slot_desc != nullptr) DCHECK(slot_desc->type().IsCollectionType());
  }

  virtual ~CollectionColumnReader() {}

  virtual bool IsStructReader() const override { return false; }

  virtual bool IsCollectionReader() const override { return true; }

  /// The repetition level indicating that the current value is the first in a new
  /// collection (meaning the last value read was the final item in the previous
  /// collection).
  int new_collection_rep_level() const { return max_rep_level() - 1; }

  /// Materializes CollectionValue into tuple slot (if materializing) and advances to next
  /// value.
  virtual bool ReadValue(MemPool* pool, Tuple* tuple) override;

  /// Same as ReadValue but does not advance repetition level. Only valid for columns not
  /// in collections.
  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) override;

  /// Implementation of ReadValueBatch for collections.
  virtual bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) override;

  /// Implementation of ReadNonRepeatedValueBatch() for collections.
  virtual bool ReadNonRepeatedValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) override;

  /// Advances all child readers to the beginning of the next collection and updates this
  /// reader's state.
  virtual bool NextLevels() override;

  /// Skips the number of encoded values specified by 'num_rows', without materilizing or
  /// decoding them.
  /// Returns true on success, false otherwise.
  virtual bool SkipRows(int64_t num_rows, int64_t skip_row_id) override;

 private:
  /// Updates this reader's def_level_, rep_level_, and pos_current_value_ based on child
  /// reader's state.
  void UpdateDerivedState();

  /// Recursively reads from children_ to assemble a single CollectionValue into
  /// 'slot'. Also advances rep_level_ and def_level_ via NextLevels().
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  inline bool ReadSlot(CollectionValue* slot, MemPool* pool);
};
} // namespace impala
