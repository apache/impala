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

#include <gtest/gtest.h>
#include <string>
#include <limits>
#include <cmath>

#include "testutil/gtest-util.h"
#include "exprs/aggregate-functions.h"
#include "exec/incr-stats-util.h"

using namespace impala;

extern string EncodeNdv(const string& ndv, bool* is_encoded);
extern string DecodeNdv(const string& ndv, bool is_encoded);

static const int DEFAULT_HLL_LEN = pow(2, AggregateFunctions::DEFAULT_HLL_PRECISION);

TEST(IncrStatsUtilTest, TestEmptyRle) {
  string test(DEFAULT_HLL_LEN, 0);

  bool is_encoded;
  const string& encoded = EncodeNdv(test, &is_encoded);
  ASSERT_EQ(8, encoded.size());
  ASSERT_TRUE(is_encoded);

  const string& decoded = DecodeNdv(encoded, is_encoded);
  ASSERT_EQ(DEFAULT_HLL_LEN, decoded.size());
  ASSERT_EQ(test, decoded);
}

TEST(IncrStatsUtilTest, TestNoEncode) {
  string test;
  for (int i = 0; i < DEFAULT_HLL_LEN; ++i) {
    test += (i % 2 == 0) ? 'A' : 'B';
  }

  bool is_encoded;
  const string& encoded = EncodeNdv(test, &is_encoded);
  ASSERT_FALSE(is_encoded);
  ASSERT_EQ(encoded, test);

  ASSERT_EQ(DecodeNdv(encoded, is_encoded), test);
}

TEST(IncrStatsUtilTest, TestEncode) {
  string test;
  for (int i = 0; i < DEFAULT_HLL_LEN; ++i) {
    test += (i < 512) ? 'A' : 'B';
  }

  bool is_encoded;
  const string& encoded = EncodeNdv(test, &is_encoded);
  ASSERT_EQ(8, encoded.size());
  ASSERT_TRUE(is_encoded);
  ASSERT_EQ(DecodeNdv(encoded, is_encoded), test);
}

void checkLowAndHighValueInt(
    const TColumnStats& stats, int expected_low, int expected_high) {
  ASSERT_TRUE(stats.low_value.__isset.int_val);
  ASSERT_TRUE(stats.high_value.__isset.int_val);
  ASSERT_EQ(expected_low, stats.low_value.int_val);
  ASSERT_EQ(expected_high, stats.high_value.int_val);
}

TEST(IncrStatsUtilTest, TestLowAndHighValueInt) {
  PerColumnStats* stat = new PerColumnStats();
  TColumnValue lv;
  TColumnValue hv;

  lv.__set_int_val(10);
  hv.__set_int_val(20);
  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 0, 0, 0, lv, hv);

  checkLowAndHighValueInt(stat->ToTColumnStats(), 10, 20);

  lv.__set_int_val(2);
  hv.__set_int_val(30);
  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 0, 0, 0, lv, hv);
  checkLowAndHighValueInt(stat->ToTColumnStats(), 2, 30);
}

void checkLowAndHighValueShort(
    const TColumnStats& stats, short expected_low, short expected_high) {
  ASSERT_TRUE(stats.low_value.__isset.short_val);
  ASSERT_TRUE(stats.high_value.__isset.short_val);
  ASSERT_EQ(expected_low, stats.low_value.short_val);
  ASSERT_EQ(expected_high, stats.high_value.short_val);
}

TEST(IncrStatsUtilTest, TestLowAndHighValueShort) {
  PerColumnStats* stat = new PerColumnStats();
  TColumnValue lv;
  TColumnValue hv;

  lv.__set_short_val(10);
  hv.__set_short_val(20);
  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 0, 0, 0, lv, hv);

  checkLowAndHighValueShort(stat->ToTColumnStats(), 10, 20);

  lv.__set_short_val(14);
  hv.__set_short_val(30);
  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 0, 0, 0, lv, hv);
  checkLowAndHighValueShort(stat->ToTColumnStats(), 10, 30);
}

/**
 * This test checks the acceptable 'new_num_null' values by the PerColumnStats.Update
 * method. In earlier releases the number of null values were not counted and the
 * 'num_nulls' was set to '-1' to indicate missing statistics. To avoid misleading
 * behavior between partition statistics created by different releases the column stats
 * should be set to '-1' when a '-1' value exists in the partition stats.
 */
TEST(IncrStatsUtilTest, TestNumNullAggregation) {
  PerColumnStats* stat = new PerColumnStats();
  ASSERT_EQ(0, stat->ToTColumnStats().num_nulls);
  TColumnValue lv;
  TColumnValue hv;

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 1, 0, 0, lv, hv);
  ASSERT_EQ(1, stat->ToTColumnStats().num_nulls);

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 0, 0, 0, lv, hv);
  ASSERT_EQ(1, stat->ToTColumnStats().num_nulls);

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 2, 0, 0, lv, hv);
  ASSERT_EQ(3, stat->ToTColumnStats().num_nulls);

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, -1, 0, 0, lv, hv);
  ASSERT_EQ(-1, stat->ToTColumnStats().num_nulls);

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 0, 0, 0, lv, hv);
  ASSERT_EQ(-1, stat->ToTColumnStats().num_nulls);

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 3, 0, 0, lv, hv);
  ASSERT_EQ(-1, stat->ToTColumnStats().num_nulls);

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, -1, 0, 0, lv, hv);
  ASSERT_EQ(-1, stat->ToTColumnStats().num_nulls);
}

/**
 * This test updates a PerColumnStats object with new partition stat values 'new_num_null'
 * and 'new_avg_width'. Then checks if the aggregated average size of the partition column
 * stat is accurate after the PerColumnStats.Finalize method has been called.
*/
TEST(IncrStatsUtilTest, TestAvgSizehAggregation) {
  PerColumnStats* stat = new PerColumnStats();
  TColumnValue lv;
  TColumnValue hv;

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 1, 4, 0, 0, 0, 0, lv, hv);
  stat->Finalize();
  ASSERT_EQ(4, stat->ToTColumnStats().avg_size);

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 2, 7, 0, 0, 0, 0, lv, hv);
  stat->Finalize();
  ASSERT_EQ(6, stat->ToTColumnStats().avg_size);

  stat->Update(string(AggregateFunctions::DEFAULT_HLL_LEN, 0), 0, 0, 0, 0, 0, 0, lv, hv);
  stat->Finalize();
  ASSERT_EQ(6, stat->ToTColumnStats().avg_size);
}
