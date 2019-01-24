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

#include "gen-cpp/parquet_types.h"
#include "exec/parquet/parquet-page-index.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

struct PageIndexRanges {
  int64_t column_index_offset;
  int64_t column_index_length;
  int64_t offset_index_offset;
  int64_t offset_index_length;
};

using RowGroupRanges = vector<PageIndexRanges>;

/// Creates a parquet::RowGroup object based on data in 'row_group_ranges'. It sets
/// the offsets and sizes of the column index and offset index members of the row group.
/// It doesn't set the member if the input value is -1.
void ConstructFakeRowGroup(const RowGroupRanges& row_group_ranges,
    parquet::RowGroup* row_group) {
  for (auto& page_index_ranges : row_group_ranges) {
    parquet::ColumnChunk col_chunk;
    if (page_index_ranges.column_index_offset != -1) {
      col_chunk.__set_column_index_offset(page_index_ranges.column_index_offset);
    }
    if (page_index_ranges.column_index_length != -1) {
      col_chunk.__set_column_index_length(page_index_ranges.column_index_length);
    }
    if (page_index_ranges.offset_index_offset != -1) {
      col_chunk.__set_offset_index_offset(page_index_ranges.offset_index_offset);
    }
    if (page_index_ranges.offset_index_length != -1) {
      col_chunk.__set_offset_index_length(page_index_ranges.offset_index_length);
    }
    row_group->columns.push_back(col_chunk);
  }
}

/// Validates that 'DeterminePageIndexRangesInRowGroup()' selects the expected file
/// offsets and sizes or returns false when the row group doesn't have a page index.
void ValidatePageIndexRange(const RowGroupRanges& row_group_ranges,
    bool expected_has_page_index, int expected_ci_start, int expected_ci_size,
    int expected_oi_start, int expected_oi_size) {
  parquet::RowGroup row_group;
  ConstructFakeRowGroup(row_group_ranges, &row_group);

  int64_t ci_start;
  int64_t ci_size;
  int64_t oi_start;
  int64_t oi_size;
  bool has_page_index = ParquetPageIndex::DeterminePageIndexRangesInRowGroup(row_group,
      &ci_start, &ci_size, &oi_start, &oi_size);
  ASSERT_EQ(expected_has_page_index, has_page_index);
  if (has_page_index) {
    EXPECT_EQ(expected_ci_start, ci_start);
    EXPECT_EQ(expected_ci_size, ci_size);
    EXPECT_EQ(expected_oi_start, oi_start);
    EXPECT_EQ(expected_oi_size, oi_size);
  }
}

/// This test constructs a couple of artificial row groups with page index offsets in
/// them. Then it validates if ParquetPageIndex::DeterminePageIndexRangeInFile() properly
/// computes the file range that contains the whole page index.
TEST(ParquetPageIndex, DeterminePageIndexRangesInRowGroup) {
  // No Column chunks
  ValidatePageIndexRange({}, false, -1, -1, -1, -1);
  // No page index at all.
  ValidatePageIndexRange({{-1, -1, -1, -1}}, false, -1, -1, -1, -1);
  // Page index for single column chunk.
  ValidatePageIndexRange({{10, 5, 15, 5}}, true, 10, 5, 15, 5);
  // Page index for two column chunks.
  ValidatePageIndexRange({{10, 5, 30, 25}, {15, 15, 50, 20}}, true, 10, 20, 30, 40);
  // Page index for second column chunk..
  ValidatePageIndexRange({{-1, -1, -1, -1}, {20, 10, 30, 25}}, true, 20, 10, 30, 25);
  // Page index for first column chunk.
  ValidatePageIndexRange({{10, 5, 15, 5}, {-1, -1, -1, -1}}, true, 10, 5, 15, 5);
  // Missing offset index for first column chunk. Gap in column index.
  ValidatePageIndexRange({{10, 5, -1, -1}, {20, 10, 30, 25}}, true, 10, 20, 30, 25);
  // Missing offset index for second column chunk.
  ValidatePageIndexRange({{10, 5, 25, 5}, {20, 10, -1, -1}}, true, 10, 20, 25, 5);
  // Three column chunks.
  ValidatePageIndexRange({{100, 10, 220, 30}, {110, 25, 250, 10}, {140, 30, 260, 40},
    {200, 10, 300, 100}}, true, 100, 110, 220, 180);
}

}

IMPALA_TEST_MAIN();
