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

#include "exec/parquet/hdfs-parquet-scanner.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

static const int64_t MIN_BUFFER_SIZE = 64 * 1024;
static const int64_t MAX_BUFFER_SIZE = 8 * 1024 * 1024;

DECLARE_int64(min_buffer_size);
DECLARE_int32(read_size);

namespace impala {

class HdfsParquetScannerTest : public testing::Test {
 public:
  virtual void SetUp() {
    // Override min/max buffer sizes picked up by DiskIoMgr.
    FLAGS_min_buffer_size = MIN_BUFFER_SIZE;
    FLAGS_read_size = MAX_BUFFER_SIZE;

    test_env_.reset(new TestEnv);
    ASSERT_OK(test_env_->Init());
  }

  virtual void TearDown() {
    test_env_.reset();
  }

 protected:
  void TestComputeIdealReservation(const vector<int64_t>& col_range_lengths,
      int64_t expected_ideal_reservation);
  void TestDivideReservation(const vector<int64_t>& col_range_lengths,
      int64_t total_col_reservation, const vector<int64_t>& expected_reservations);

  boost::scoped_ptr<TestEnv> test_env_;
};

/// Test the ComputeIdealReservation returns 'expected_ideal_reservation' for a list
/// of columns with 'col_range_lengths'.
void HdfsParquetScannerTest::TestComputeIdealReservation(
    const vector<int64_t>& col_range_lengths, int64_t expected_ideal_reservation) {
  EXPECT_EQ(expected_ideal_reservation,
      HdfsParquetScanner::ComputeIdealReservation(col_range_lengths));
}

TEST_F(HdfsParquetScannerTest, ComputeIdealReservation) {
  // Should round up to nearest power-of-two buffer size if < max scan range buffer.
  TestComputeIdealReservation({0}, MIN_BUFFER_SIZE);
  TestComputeIdealReservation({1}, MIN_BUFFER_SIZE);
  TestComputeIdealReservation({MIN_BUFFER_SIZE - 1}, MIN_BUFFER_SIZE);
  TestComputeIdealReservation({MIN_BUFFER_SIZE}, MIN_BUFFER_SIZE);
  TestComputeIdealReservation({MIN_BUFFER_SIZE + 2}, 2 * MIN_BUFFER_SIZE);
  TestComputeIdealReservation({4 * MIN_BUFFER_SIZE + 1234}, 8 * MIN_BUFFER_SIZE);
  TestComputeIdealReservation({MAX_BUFFER_SIZE - 10}, MAX_BUFFER_SIZE);
  TestComputeIdealReservation({MAX_BUFFER_SIZE}, MAX_BUFFER_SIZE);

  // Should round to nearest max I/O buffer size if >= max scan range buffer, up to 3
  // buffers.
  TestComputeIdealReservation({MAX_BUFFER_SIZE + 1}, 2 * MAX_BUFFER_SIZE);
  TestComputeIdealReservation({MAX_BUFFER_SIZE * 2 - 1}, 2 * MAX_BUFFER_SIZE);
  TestComputeIdealReservation({MAX_BUFFER_SIZE * 2}, 2 * MAX_BUFFER_SIZE);
  TestComputeIdealReservation({MAX_BUFFER_SIZE * 2 + 1}, 3 * MAX_BUFFER_SIZE);
  TestComputeIdealReservation({MAX_BUFFER_SIZE * 3 + 1}, 3 * MAX_BUFFER_SIZE);
  TestComputeIdealReservation({MAX_BUFFER_SIZE * 100 + 27}, 3 * MAX_BUFFER_SIZE);

  // Ideal reservations from multiple ranges are simply added together.
  TestComputeIdealReservation({1, 2}, 2 * MIN_BUFFER_SIZE);
  TestComputeIdealReservation(
      {MAX_BUFFER_SIZE, MAX_BUFFER_SIZE - 1}, 2 * MAX_BUFFER_SIZE);
  TestComputeIdealReservation(
      {MAX_BUFFER_SIZE, MIN_BUFFER_SIZE + 1}, MAX_BUFFER_SIZE + 2 * MIN_BUFFER_SIZE);
  TestComputeIdealReservation(
      {MAX_BUFFER_SIZE, MAX_BUFFER_SIZE * 128}, 4 * MAX_BUFFER_SIZE);
  TestComputeIdealReservation(
      {MAX_BUFFER_SIZE * 7, MAX_BUFFER_SIZE * 128, MAX_BUFFER_SIZE * 1000},
      3L * 3L * MAX_BUFFER_SIZE);

  // Test col size that doesn't fit in int32.
  TestComputeIdealReservation({MAX_BUFFER_SIZE * 1024L}, 3L * MAX_BUFFER_SIZE);

  // Test sum of reservations that doesn't fit in int32.
  vector<int64_t> col_range_lengths;
  const int64_t LARGE_NUM_RANGES = 10000;
  col_range_lengths.reserve(LARGE_NUM_RANGES);
  for (int i = 0; i < LARGE_NUM_RANGES; ++i) {
    col_range_lengths.push_back(4 * MAX_BUFFER_SIZE);
  }
  TestComputeIdealReservation(col_range_lengths, LARGE_NUM_RANGES * 3L * MAX_BUFFER_SIZE);
}

/// Test that DivideReservationBetweenColumns() returns 'expected_reservations' for
/// inputs 'col_range_lengths' and 'total_col_reservation'.
void HdfsParquetScannerTest::TestDivideReservation(
    const vector<int64_t>& col_range_lengths, int64_t total_col_reservation,
    const vector<int64_t>& expected_reservations) {
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
