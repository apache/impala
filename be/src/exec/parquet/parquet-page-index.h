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

#include "runtime/scoped-buffer.h"

namespace parquet {
class ColumnChunk;
class ColumnIndex;
class OffsetIndex;
}

namespace impala {

class HdfsParquetScanner;

/// Helper class for reading the Parquet page index. It allocates a buffer to hold the
/// raw bytes of the page index. It provides helper methods to deserialize the relevant
/// parts.
class ParquetPageIndex {
public:
  ParquetPageIndex(HdfsParquetScanner* scanner);

  /// It reads the raw bytes of a page index belonging to a specific row group. It stores
  /// the bytes in an internal buffer.
  /// It expects that the layout of the page index conforms to the specification, i.e.
  /// column indexes come before offset indexes. Otherwise it returns an error. (Impala
  /// and Parquet-MR produce conforming page index layouts).
  /// It needs to be called before the serialization methods.
  Status ReadAll(int row_group_idx);

  /// Deserializes a ColumnIndex object for the given column chunk.
  Status DeserializeColumnIndex(const parquet::ColumnChunk& col_chunk,
      parquet::ColumnIndex* column_index);

  /// Deserializes an OffsetIndex object for the given column chunk.
  Status DeserializeOffsetIndex(const parquet::ColumnChunk& col_chunk,
      parquet::OffsetIndex* offset_index);

  /// Determines the column index and offset index ranges for the given row group.
  /// Returns true when at least a partial column index and an offset index are found.
  /// Returns false when there is absolutely no column index or offset index for the row
  /// group.
  static bool DeterminePageIndexRangesInRowGroup(
      const parquet::RowGroup& row_group, int64_t* column_index_start,
      int64_t* column_index_size, int64_t* offset_index_start,
      int64_t* offset_index_size);

  /// Releases resources held by this object.
  void Release() { page_index_buffer_.Release(); }

  /// Returns true if the page index buffer is empty.
  bool IsEmpty() { return page_index_buffer_.Size() == 0; }
private:
  /// The scanner that created this object.
  HdfsParquetScanner* scanner_;

  /// Buffer to hold the raw bytes of the page index.
  ScopedBuffer page_index_buffer_;

  /// File offsets and sizes of the page Index.
  int64_t column_index_base_offset_;
  int64_t column_index_size_;
  int64_t offset_index_base_offset_;
  int64_t offset_index_size_;
};

}
