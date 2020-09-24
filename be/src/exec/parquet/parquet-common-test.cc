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


#include "exec/parquet/parquet-common.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

using RangeVec = vector<RowRange>;

void ValidateRanges(RangeVec skip_ranges, int num_rows, const RangeVec& expected,
    bool should_succeed = true) {
  RangeVec result;
  bool success = ComputeCandidateRanges(num_rows, &skip_ranges, &result);
  EXPECT_EQ(should_succeed, success);
  if (success) {
    EXPECT_EQ(expected, result);
  }
}

void ValidateRangesError(RangeVec skip_ranges, int num_rows, const RangeVec& expected) {
  ValidateRanges(skip_ranges, num_rows, expected, false);
}

/// This test exercises the logic of ComputeCandidateRanges() with various
/// inputs. ComputeCandidateRanges() determines the row ranges we need to scan
/// in a Parquet file based on the total row count and a set of ranges that we
/// need to skip.
TEST(ParquetCommon, ComputeCandidateRanges) {
  ValidateRanges({}, -1, {});
  ValidateRanges({{0, 5}}, 10, {{6, 9}});
  ValidateRanges({{6, 9}}, 10, {{0, 5}});
  ValidateRanges({{0, 9}}, 10, {});
  ValidateRanges({{2, 4}, {2, 4}}, 10, {{0, 1}, {5, 9}});
  ValidateRanges({{2, 4}, {6, 6}}, 10, {{0, 1}, {5, 5}, {7, 9}});
  ValidateRanges({{2, 4}, {3, 7}}, 10, {{0, 1}, {8, 9}});
  ValidateRanges({{2, 6}, {1, 8}}, 10, {{0, 0}, {9, 9}});
  ValidateRanges({{1, 2}, {2, 5}, {7, 8}}, 10, {{0, 0}, {6, 6}, {9, 9}});
  ValidateRanges({{0, 2}, {6, 8}, {3, 5}}, 10, {{9, 9}});
  ValidateRanges({{1, 2}, {1, 4}, {1, 8}}, 10, {{0, 0}, {9, 9}});
  ValidateRanges({{7, 8}, {1, 8}, {3, 8}}, 10, {{0, 0}, {9, 9}});
  // Error cases:
  // Range starts at negative number
  ValidateRangesError({{-1, 1}}, 10, {});
  // Range is not a subrange of [0..num_rows)
  ValidateRangesError({{2, 12}}, 10, {});
  // First > last
  ValidateRangesError({{6, 3}}, 10, {});
}

void ValidatePages(const vector<int64_t>& first_row_indexes, const RangeVec& ranges,
    int64_t num_rows, const vector<int>& expected_page_indexes,
    bool should_succeed = true) {
  vector<parquet::PageLocation> page_locations;
  for (int64_t first_row_index : first_row_indexes) {
    parquet::PageLocation page_loc;
    page_loc.first_row_index = first_row_index;
    if (first_row_index != -1) {
      // first_row_index == -1 means empty page.
      page_loc.compressed_page_size = 42;
    }
    page_locations.push_back(page_loc);
  }
  vector<int> candidate_pages;
  bool success = ComputeCandidatePages(page_locations, ranges, num_rows,
      &candidate_pages);
  EXPECT_EQ(should_succeed, success);
  if (success) {
    EXPECT_EQ(expected_page_indexes, candidate_pages);
  }
}

void ValidatePagesError(const vector<int64_t>& first_row_indexes, const RangeVec& ranges,
    int64_t num_rows, const vector<int>& expected_page_indexes) {
  ValidatePages(first_row_indexes, ranges, num_rows, expected_page_indexes, false);
}

/// This test exercises the logic of ComputeCandidatePages(). It creates fake vectors
/// of page location objects and candidate ranges then checks whether the right pages
/// were selected.
TEST(ParquetCommon, ComputeCandidatePages) {
  ValidatePages({0}, {}, 10, {});
  ValidatePages({0}, {{2, 3}}, 10, {0});
  ValidatePages({0}, {{0, 9}}, 10, {0});
  ValidatePages({0, 10, 20, 50, 70}, {{0, 9}}, 100, {0});
  ValidatePages({0, 10, 20, 50, 70}, {{5, 15}}, 100, {0, 1});
  ValidatePages({0, 10, 20, 50, 70}, {{5, 15}}, 100, {0, 1});
  ValidatePages({0, 10, 20, 50, 70}, {{5, 15}, {21, 50}}, 100, {0, 1, 2, 3});
  ValidatePages({0, 10, 20, 50, 70}, {{90, 92}}, 100, {4});
  ValidatePages({0, 10, 20, 50, 70},
                {{0, 9}, {10, 19}, {20, 49}, {50, 69}, {70, 99}}, 100, {0, 1, 2, 3, 4});
  ValidatePages({0, 10, 20, 50, 70}, {{5, 75}}, 100, {0, 1, 2, 3, 4});
  ValidatePages({0, 10, 20, 50, 70}, {{9, 10}, {69, 70}}, 100, {0, 1, 3, 4});
  ValidatePages({0, 10, 20, 50, 70}, {{9, 9}, {10, 10}, {50, 50}}, 100, {0, 1, 3});
  ValidatePages({0}, {{0, 9LL + INT_MAX}}, 100LL + INT_MAX, {0});
  ValidatePages({0LL, 10LL + INT_MAX, 20LL + INT_MAX, 50LL + INT_MAX, 70LL + INT_MAX},
      {{0LL, 9LL + INT_MAX}}, 100LL + INT_MAX, {0});
  ValidatePages({0LL, 10LL + UINT_MAX, 20LL + UINT_MAX, 50LL + UINT_MAX, 70LL + UINT_MAX},
        {{0LL, 9LL + UINT_MAX}}, 100LL + UINT_MAX, {0});
  // Empty pages are ignored
  ValidatePages({0, -1}, {{0, 10}}, 15, {0});
  ValidatePages({-1, 0}, {{0, 10}}, 15, {1});
  ValidatePages({-1, 0, -1, -1, 10, -1}, {{0, 10}}, 15, {1, 4});
  // Error cases:
  // Negative first row index.
  ValidatePagesError({-2, 0, 10}, {{0, 10}}, 15, {0});
  // First row index greater then number of rows.
  ValidatePagesError({5, 10, 15}, {{0, 10}}, 10, {0});
  // First row indexes are not in order.
  ValidatePagesError({0, 5, 3}, {{0, 10}}, 10, {0});
  // Row ranges don't overlap with pages.
  ValidatePagesError({0, 5, 10}, {{15, 20}}, 12, {0});
}

}
