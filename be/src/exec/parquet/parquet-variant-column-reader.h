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

#include "exec/parquet/parquet-struct-column-reader.h"
#include "gutil/strings/substitute.h"

namespace impala {

/// Reads unshredded variant columns from Parquet files. An unshredded variant is stored
/// as a Parquet group with two BINARY children: "metadata" (field name dictionary) and
/// "value" (the encoded variant value).
///
/// This reader inherits from StructColumnReader since the physical Parquet layout is
/// identical to a struct with two binary fields. The only difference is the type check
/// and the slot size (24 bytes = two StringValues).
///
/// For shredded variants (future), this reader will be extended to handle additional
/// typed children beyond metadata+value, enabling projection pushdown.
class VariantColumnReader : public StructColumnReader {
 public:
  VariantColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc) : StructColumnReader(parent, node, slot_desc) {
    if (slot_desc != nullptr) DCHECK(slot_desc->type().IsVariantType());
  }

  virtual ~VariantColumnReader() {}

  virtual bool IsStructReader() const override { return false; }

  /// Unlike a real struct a variant can be late materialized: HasStructReader() is false
  /// for it, because IsStructReader() is false and its children are scalar readers, so
  /// HdfsParquetScanner::HasStructColumnReader() does not exclude it. The children are
  /// read through the non-batched interface. If they have read the levels of the next
  /// row ahead, BaseScalarColumnReader::SkipRows() undoes that first. Ending with
  /// NextLevels() reads the levels of the next row again and refreshes our own levels
  /// from children_[0], including ParquetLevel::ROW_GROUP_END, which the scanner polls
  /// on column_readers_[0].
  virtual bool SkipRows(int64_t num_rows, int64_t skip_row_id) override {
    DCHECK_EQ(children_.size(), 2);
    // Only top level variants exist so far, see IMPALA-15132, and CreateColumnReaders()
    // rejects a VARIANT that is a repeated group. A nested one would have to compose
    // with its parent through next_levels_consumed_, which is untested.
    DCHECK_EQ(max_rep_level(), 0);
    next_levels_consumed_ = false;
    for (ParquetColumnReader* child : children_) {
      if (!child->SkipRows(num_rows, skip_row_id)) return false;
    }
    return StructColumnReader::NextLevels();
  }

  /// Our levels already mirror children_[0], so by the time the scanner declares the row
  /// group finished we must be at its end too. If we are not, this column has more rows
  /// than the columns it is read with; report that instead of letting the children drop
  /// the row they read ahead, which would also leave RowGroupAtEnd() false forever.
  virtual bool SetRowGroupAtEnd() override {
    if (UNLIKELY(!RowGroupAtEnd())) {
      parent_->parse_status_.MergeStatus(Status(strings::Substitute(
          "Corrupt Parquet file '$0': column '$1' has more rows than the other columns "
          "of the row group", filename(), schema_element().name)));
      return false;
    }
    return ComplexColumnReader::SetRowGroupAtEnd();
  }
};

} // namespace impala
