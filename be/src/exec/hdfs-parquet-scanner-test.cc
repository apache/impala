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

#include "exec/hdfs-parquet-scanner.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

static const int64_t MIN_BUFFER_SIZE = 64 * 1024;
static const int64_t MAX_BUFFER_SIZE = 8 * 1024 * 1024;

namespace impala {

class HdfsParquetScannerTest : public testing::Test {
 protected:
  void TestDivideReservation(const vector<int64_t>& col_range_lengths,
      int64_t total_col_reservation, const vector<int64_t>& expected_reservations);
};

/// Test that DivideReservationBetweenColumns() returns 'expected_reservations' for
/// inputs 'col_range_lengths' and 'total_col_reservation'.
void HdfsParquetScannerTest::TestDivideReservation(const vector<int64_t>& col_range_lengths,
      int64_t total_col_reservation, const vector<int64_t>& expected_reservations) {
  vector<pair<int, int64_t>> reservations =
      HdfsParquetScanner::DivideReservationBetweenColumnsHelper(
      MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, col_range_lengths, total_col_reservation);
  for (int i = 0; i < reservations.size(); ++i) {
    LOG(INFO) << i << " " << reservations[i].first << " " << reservations[i].second;
  }
  EXPECT_EQ(reservations.size(), expected_reservations.size());
  vector<bool> present(expected_reservations.size(), false);
  for (auto& reservation: reservations) {
    // Ensure that each appears exactly once.
    EXPECT_FALSE(present[reservation.first]);
    present[reservation.first] = true;
    EXPECT_EQ(expected_reservations[reservation.first], reservation.second)
        << reservation.first;
  }
}

TEST_F(HdfsParquetScannerTest, DivideReservation) {
  // Test a long scan ranges with lots of memory - should allocate 3 max-size
  // buffers per range.
  TestDivideReservation({100 * 1024 * 1024}, 50 * 1024 * 1024, {3 * MAX_BUFFER_SIZE});
  TestDivideReservation({100 * 1024 * 1024, 50 * 1024 * 1024}, 100 * 1024 * 1024,
        {3 * MAX_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE});

  // Long scan ranges, not enough memory for 3 buffers each. Should only allocate
  // max-sized buffers, preferring the longer scan range.
  TestDivideReservation({50 * 1024 * 1024, 100 * 1024 * 1024}, 5 * MAX_BUFFER_SIZE,
        {2 * MAX_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE});
  TestDivideReservation({50 * 1024 * 1024, 100 * 1024 * 1024},
        5 * MAX_BUFFER_SIZE + MIN_BUFFER_SIZE,
        {2 * MAX_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE});
  TestDivideReservation({50 * 1024 * 1024, 100 * 1024 * 1024}, 6 * MAX_BUFFER_SIZE - 1,
        {2 * MAX_BUFFER_SIZE, 3 * MAX_BUFFER_SIZE});

  // Test a short range with lots of memory - should round up buffer size.
  TestDivideReservation({100 * 1024}, 50 * 1024 * 1024, {128 * 1024});

  // Test a range << MIN_BUFFER_SIZE - should round up to buffer size.
  TestDivideReservation({13}, 50 * 1024 * 1024, {MIN_BUFFER_SIZE});

  // Test long ranges with limited memory.
  TestDivideReservation({100 * 1024 * 1024}, 100 * 1024, {MIN_BUFFER_SIZE});
  TestDivideReservation({100 * 1024 * 1024}, MIN_BUFFER_SIZE, {MIN_BUFFER_SIZE});
  TestDivideReservation({100 * 1024 * 1024}, 2 * MIN_BUFFER_SIZE, {2 * MIN_BUFFER_SIZE});
  TestDivideReservation({100 * 1024 * 1024}, MAX_BUFFER_SIZE - 1, {MAX_BUFFER_SIZE / 2});
  TestDivideReservation({100 * 1024 * 1024, 1024 * 1024, MIN_BUFFER_SIZE},
      3 * MIN_BUFFER_SIZE, {MIN_BUFFER_SIZE, MIN_BUFFER_SIZE, MIN_BUFFER_SIZE});

  // Test a mix of scan range lengths larger than and smaller than the max I/O buffer
  // size. Long ranges get allocated most memory.
  TestDivideReservation(
      {15145047, 5019635, 5019263, 15145047, 15145047, 5019635, 5019263, 317304},
      25165824,
      {8388608, 2097152, 524288, 8388608, 4194304, 1048576, 262144, 262144});
}

}

IMPALA_TEST_MAIN();
