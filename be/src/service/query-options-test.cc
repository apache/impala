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

#include "service/query-options.h"

#include <boost/lexical_cast.hpp>
#include <inttypes.h>
#include <string>
#include <gutil/strings/substitute.h>

#include "runtime/runtime-filter.h"
#include "testutil/gtest-util.h"
#include "util/mem-info.h"

using namespace boost;
using namespace impala;
using namespace std;
using namespace strings;

TEST(QueryOptions, SetBloomSize) {
  TQueryOptions options;
  vector<pair<string, int*>> option_list = {
    {"RUNTIME_BLOOM_FILTER_SIZE", &options.runtime_bloom_filter_size},
    {"RUNTIME_FILTER_MAX_SIZE", &options.runtime_filter_max_size},
    {"RUNTIME_FILTER_MIN_SIZE", &options.runtime_filter_min_size}};
  for (const auto& option: option_list) {

    // The upper and lower bound of the allowed values:
    EXPECT_FALSE(SetQueryOption(option.first,
        lexical_cast<string>(RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE - 1), &options,
        NULL)
        .ok());

    EXPECT_FALSE(SetQueryOption(option.first,
        lexical_cast<string>(RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE + 1), &options,
        NULL)
        .ok());

    EXPECT_OK(SetQueryOption(option.first,
        lexical_cast<string>(RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE), &options, NULL));
    EXPECT_EQ(RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE, *option.second);

    EXPECT_OK(SetQueryOption(option.first,
        lexical_cast<string>(RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE), &options, NULL));
    EXPECT_EQ(RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE, *option.second);

    // Parsing memory values works in a reasonable way:
    EXPECT_OK(SetQueryOption(option.first, "1MB", &options, NULL));
    EXPECT_EQ(1 << 20, *option.second);

    // Bloom filters cannot occupy a percentage of memory:
    EXPECT_FALSE(SetQueryOption(option.first, "10%", &options, NULL).ok());
  }
}

TEST(QueryOptions, SetFilterWait) {
  TQueryOptions options;

  // The upper and lower bound of the allowed values:
  EXPECT_FALSE(SetQueryOption("RUNTIME_FILTER_WAIT_TIME_MS", "-1", &options, NULL).ok());

  EXPECT_FALSE(SetQueryOption("RUNTIME_FILTER_WAIT_TIME_MS",
      lexical_cast<string>(numeric_limits<int32_t>::max() + 1), &options, NULL).ok());

  EXPECT_OK(SetQueryOption("RUNTIME_FILTER_WAIT_TIME_MS", "0", &options, NULL));
  EXPECT_EQ(0, options.runtime_filter_wait_time_ms);

  EXPECT_OK(SetQueryOption("RUNTIME_FILTER_WAIT_TIME_MS",
      lexical_cast<string>(numeric_limits<int32_t>::max()), &options, NULL));
  EXPECT_EQ(numeric_limits<int32_t>::max(), options.runtime_filter_wait_time_ms);
}

TEST(QueryOptions, MaxScanRangeLength) {
  vector<pair<string, int64_t>> vals = {{"4GB", 4L * 1024 * 1024 * 1024},
                                        {"-1M", -1},
                                        {"0B", 0},
                                        {"1024", 1024},
                                        {"9223372036854775807", 9223372036854775807},
                                        {"9223372036854775808", -1}, // 2**63
                                        {"Not a number!", -1}};
  for (const auto& val: vals) {
    TQueryOptions options;
    QueryOptionsMask mask;
    Status status = ParseQueryOptions(
        Substitute("MAX_SCAN_RANGE_LENGTH=$0", val.first), &options, &mask);
    int64_t expected = val.second;
    if (expected == -1) {
      EXPECT_FALSE(status.ok());
    } else {
      EXPECT_OK(status);
      EXPECT_EQ(expected, options.max_scan_range_length);
    }
  }
}

TEST(QueryOptions, ParseQueryOptions) {
  QueryOptionsMask expectedMask;
  expectedMask.set(TImpalaQueryOptions::NUM_NODES);
  expectedMask.set(TImpalaQueryOptions::MEM_LIMIT);

  {
    TQueryOptions options;
    QueryOptionsMask mask;
    EXPECT_OK(ParseQueryOptions("num_nodes=1,mem_limit=42", &options, &mask));
    EXPECT_EQ(options.num_nodes, 1);
    EXPECT_EQ(options.mem_limit, 42);
    EXPECT_EQ(mask, expectedMask);
  }

  {
    TQueryOptions options;
    QueryOptionsMask mask;
    Status status = ParseQueryOptions("num_nodes=1,mem_limit:42,foo=bar,mem_limit=42",
        &options, &mask);
    EXPECT_EQ(options.num_nodes, 1);
    EXPECT_EQ(options.mem_limit, 42);
    EXPECT_EQ(mask, expectedMask);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.msg().details().size(), 2);
  }
}

IMPALA_TEST_MAIN();
