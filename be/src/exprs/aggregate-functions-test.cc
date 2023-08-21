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

#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/variance.hpp>

#include "exprs/aggregate-functions.h"
#include "testutil/gtest-util.h"
#include "udf/udf.h"
#include "udf/uda-test-harness.h"
#include "util/decimal-constants.h"

#include "common/names.h"

namespace tag = boost::accumulators::tag;
using boost::accumulators::accumulator_set;
using boost::accumulators::stats;
using boost::accumulators::variance;
using boost::algorithm::is_any_of;
using boost::algorithm::trim;
using namespace impala;
using namespace impala_udf;

template <int RANGE_START, int RANGE_END>
bool CheckAppxMedian(const IntVal& actual, const IntVal& expected) {
  return actual.val >= RANGE_START && actual.val <= RANGE_END;
}

bool CheckHistogramDistribution(const StringVal& actual,
    const StringVal& max_expected_stdev) {
  string result(reinterpret_cast<char*>(actual.ptr), actual.len);
  vector<string> str_vals;
  split(str_vals, result, is_any_of(","));

  accumulator_set<int, stats<tag::variance>> acc;
  int prev_val = -1;
  for (string& s: str_vals) {
    trim(s);
    int val = lexical_cast<int>(s);
    if (prev_val != -1) acc(val - prev_val);
    prev_val = val;
  }
  double actual_stdev = sqrt(variance(acc));
  string expected_str(reinterpret_cast<char*>(max_expected_stdev.ptr),
      max_expected_stdev.len);
  return actual_stdev < lexical_cast<double>(expected_str);
}

// TODO: Add other datatypes
TEST(HistogramTest, TestInt) {
  UdaTestHarness<StringVal, StringVal, IntVal> test_histogram(
      AggregateFunctions::ReservoirSampleInit<IntVal>,
      AggregateFunctions::ReservoirSampleUpdate<IntVal>,
      AggregateFunctions::ReservoirSampleMerge<IntVal>,
      AggregateFunctions::ReservoirSampleSerialize<IntVal>,
      AggregateFunctions::HistogramFinalize<IntVal>);
  UdaTestHarness<IntVal, StringVal, IntVal> test_median(
      AggregateFunctions::ReservoirSampleInit<IntVal>,
      AggregateFunctions::ReservoirSampleUpdate<IntVal>,
      AggregateFunctions::ReservoirSampleMerge<IntVal>,
      AggregateFunctions::ReservoirSampleSerialize<IntVal>,
      AggregateFunctions::AppxMedianFinalize<IntVal>);
  const int NUM_BUCKETS = 100;
  const int INPUT_SIZE = NUM_BUCKETS * 1000;

  // All input values are 1, result should be constant.
  {
    vector<IntVal> input(INPUT_SIZE, 1);
    char expected[] = "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "
      "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "
      "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "
      "1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1";
    EXPECT_TRUE(test_histogram.Execute(input, StringVal(&expected[0])))
        << test_histogram.GetErrorMsg();
  }

  // Now check input values ranging from 0 to 100,000. Each bucket should have 1000
  // values, i.e. bucket i should approximately contain values [100*i, 100*(i+1)]. We
  // check the distribution of the deltas between histogram values is not too large.
  // TODO: Add more deterministic test cases
  {
    vector<IntVal> input;
    input.reserve(INPUT_SIZE);
    for (int i = 0; i < INPUT_SIZE; ++i) input.push_back(i);
    test_histogram.SetResultComparator(CheckHistogramDistribution);
    StringVal max_expected_stdev = StringVal("100.0");
    EXPECT_TRUE(test_histogram.Execute(input, max_expected_stdev))
        << test_histogram.GetErrorMsg();

    test_median.SetResultComparator(CheckAppxMedian<45000,55000>);
    EXPECT_TRUE(test_median.Execute(input, IntVal()))
        << test_median.GetErrorMsg();
  }
}

TEST(HistogramTest, TestDecimal) {
  UdaTestHarness<StringVal, StringVal, DecimalVal> test(
      AggregateFunctions::ReservoirSampleInit<DecimalVal>,
      AggregateFunctions::ReservoirSampleUpdate<DecimalVal>,
      AggregateFunctions::ReservoirSampleMerge<DecimalVal>,
      AggregateFunctions::ReservoirSampleSerialize<DecimalVal>,
      AggregateFunctions::HistogramFinalize<DecimalVal>);
  const int NUM_BUCKETS = 100;
  const int INPUT_SIZE = NUM_BUCKETS * 1000;

  // All input values are x, result should be constant.
  {
    vector<DecimalVal> input;
    input.reserve(INPUT_SIZE);
    __int128_t val = MAX_UNSCALED_DECIMAL16;
    stringstream ss;
    for (int i = 0; i < INPUT_SIZE; ++i) input.push_back(DecimalVal(val));
    for (int i = 0; i < NUM_BUCKETS; ++i) {
      ss << val;
      if (i < NUM_BUCKETS - 1) ss << ", ";
    }
    EXPECT_TRUE(test.Execute(input, StringVal(ss.str().c_str()))) << test.GetErrorMsg();
  }

  {
    vector<DecimalVal> input;
    input.reserve(INPUT_SIZE);
    for (int i = 0; i < INPUT_SIZE; ++i) input.push_back(DecimalVal(i));
    test.SetResultComparator(CheckHistogramDistribution);
    StringVal max_expected_stdev = StringVal("100.0");
    EXPECT_TRUE(test.Execute(input, max_expected_stdev)) << test.GetErrorMsg();
  }
}

TEST(HistogramTest, TestString) {
  UdaTestHarness<StringVal, StringVal, StringVal> test(
      AggregateFunctions::ReservoirSampleInit<StringVal>,
      AggregateFunctions::ReservoirSampleUpdate<StringVal>,
      AggregateFunctions::ReservoirSampleMerge<StringVal>,
      AggregateFunctions::ReservoirSampleSerialize<StringVal>,
      AggregateFunctions::HistogramFinalize<StringVal>);
  const int NUM_BUCKETS = 100;
  const int INPUT_SIZE = NUM_BUCKETS * 1000;

  // All input values are x, result should be constant.
  vector<StringVal> input;
  input.reserve(INPUT_SIZE);
  for (int i = 0; i < INPUT_SIZE; ++i) input.push_back(StringVal("x"));
  char expected[] = "x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, "
      "x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, "
      "x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, "
      "x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x";
  EXPECT_TRUE(test.Execute(input, StringVal(&expected[0]))) << test.GetErrorMsg();
}

TEST(DsThetaSketch, DataToSketch) {
  UdaTestHarness<BigIntVal, StringVal, IntVal> test(AggregateFunctions::DsThetaInit,
      AggregateFunctions::DsThetaUpdate<IntVal>, AggregateFunctions::DsThetaMerge,
      AggregateFunctions::DsThetaSerialize, AggregateFunctions::DsThetaFinalize);
  std::vector<IntVal> input;

  EXPECT_TRUE(test.Execute(input, BigIntVal(0)))
      << "DsThetaSketch empty: " << test.GetErrorMsg();

  for (int key = 0; key < 6; key++) input.push_back(key);

  EXPECT_TRUE(test.Execute(input, BigIntVal(6)))
      << "DsThetaSketch: " << test.GetErrorMsg();
}

IMPALA_TEST_MAIN();
