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

/// A struct is not directly represented in a Parquet file, hence a StructColumnReader
/// delegates the actual reading of values to it's children.
class StructColumnReader : public ComplexColumnReader {
 public:
  StructColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc) : ComplexColumnReader(parent, node, slot_desc) {
    DCHECK(!node.children.empty());
    if (slot_desc != nullptr) DCHECK(slot_desc->type().IsStructType());
  }

  virtual ~StructColumnReader() {}

  /// Calls ReadValue() with 'IN_COLLECTION' = true as template parameter.
  virtual bool ReadValue(MemPool* pool, Tuple* tuple) override;

  /// Calls ReadValue() with 'IN_COLLECTION' = false as template parameter.
  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) override;

  /// Reads a batch of values and assumes that this column reader has a collection column
  /// reader parent. Note, this will delegate the reading to the children of the struct
  /// and their value are read in a non-batched manner calling ReadValue().
  virtual bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) override;

  /// Similar as above but this expects that this column reader doesn't have a collection
  /// column reader parent.
  virtual bool ReadNonRepeatedValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) override;

  /// Calls NextLevels() on each children and then sets the def_level_, rep_level_
  /// members. Returns false if any of the NextLevels() call returns false.
  virtual bool NextLevels() override;

  virtual bool IsStructReader() const override { return true; }

  virtual bool IsCollectionReader() const override { return false; }

  /// Skips the number of encoded values specified by 'num_rows', without materilizing or
  /// decoding them.
  /// Returns true on success, false otherwise.
  virtual bool SkipRows(int64_t num_rows, int64_t skip_row_id) override;

  void SetNullSlot(Tuple* tuple) override {
    for (ParquetColumnReader* child : children_) child->SetNullSlot(tuple);
    tuple->SetNull(DCHECK_NOTNULL(slot_desc_)->null_indicator_offset());
  }

 private:
  /// Returns true if the struct represented by this column reader has a collection
  /// parent that is null.
  template <bool IN_COLLECTION>
  bool HasNullCollectionAncestor() const;

  /// Helper function for ReadValueBatch() and ReadNonRepeatedValueBatch() functions.
  template <bool IN_COLLECTION>
  bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* RESTRICT tuple_mem, int* RESTRICT num_values) RESTRICT;

  /// Calls ReadValue() for all the children of this StructColumnReader. NextLevels()
  /// should be called before the first usage of this function to initialize def_level_
  /// and rep_level_ members. After the first call this function will take care itself
  /// of having the mentioned members up-to-date. Takes care of setting the struct to
  /// null in case the def_level_ of the children says so. Returns false if any of the
  /// ReadValue() or NextLevels() calls fail.
  /// 'read_row' is set to true if running this function actually resulted in filling a
  /// value in 'tuple'. E.g. If there is a collection parent that is null then there won't
  /// be any values written into 'tuple'.
  template <bool IN_COLLECTION>
  bool ReadValue(MemPool* pool, Tuple* tuple, bool* read_row);
};
} // namespace impala