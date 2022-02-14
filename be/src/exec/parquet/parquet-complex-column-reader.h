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

#include <vector>

#include "exec/parquet/parquet-column-readers.h"

namespace impala {

/// Abstract class to hold common functionality between the complex column readers, the
/// CollectionColumnReader and the StructColumnReader.
class ComplexColumnReader : public ParquetColumnReader {
public:
  vector<ParquetColumnReader*>* children() { return &children_; }

  virtual void Close(RowBatch* row_batch) override;

  virtual bool SetRowGroupAtEnd() override {
    DCHECK(!children_.empty());
    for (int c = 0; c < children_.size(); ++c) {
      if (!children_[c]->SetRowGroupAtEnd()) return false;
    }
    return true;
  }

  /// Returns the index of the row that was processed most recently.
  virtual int64_t LastProcessedRow() const override {
    DCHECK(!children_.empty());
    return children_[0]->LastProcessedRow();
  }

  virtual bool IsComplexReader() const override { return true; }

  virtual bool HasStructReader() const override;

  /// This is called once for each row group in the file.
  void Reset() {
    def_level_ = ParquetLevel::INVALID_LEVEL;
    rep_level_ = ParquetLevel::INVALID_LEVEL;
    pos_current_value_ = ParquetLevel::INVALID_POS;
  }

protected:
  ComplexColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
      : ParquetColumnReader(parent, node, slot_desc) {
    if (slot_desc != nullptr) DCHECK(slot_desc->type().IsComplexType());
  }

  /// Column readers of fields contained within this complex column. There is at least one
  /// child reader per complex reader. Child readers either materialize slots in the
  /// complex item tuples, or there is a single child reader that does not materialize
  /// any slot and is only used by this reader to read def and rep levels.
  std::vector<ParquetColumnReader*> children_;
};
} // namespace impala
