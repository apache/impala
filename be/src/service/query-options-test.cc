// Copyright 2016 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "service/query-options.h"

#include <boost/lexical_cast.hpp>
#include <inttypes.h>
#include <gtest/gtest.h>
#include <string>

#include "runtime/runtime-filter.h"
#include "testutil/gtest-util.h"
#include "util/mem-info.h"

using namespace boost;
using namespace impala;
using namespace std;

TEST(QueryOptions, SetBloomSize) {
  TQueryOptions options;

  // The upper and lower bound of the allowed values:
  EXPECT_FALSE(SetQueryOption("RUNTIME_BLOOM_FILTER_SIZE",
     lexical_cast<string>(RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE - 1), &options, NULL)
     .ok());

  EXPECT_FALSE(SetQueryOption("RUNTIME_BLOOM_FILTER_SIZE",
      lexical_cast<string>(RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE + 1), &options, NULL)
      .ok());

  EXPECT_OK(SetQueryOption("RUNTIME_BLOOM_FILTER_SIZE",
      lexical_cast<string>(RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE), &options, NULL));
  EXPECT_EQ(RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE, options.runtime_bloom_filter_size);

  EXPECT_OK(SetQueryOption("RUNTIME_BLOOM_FILTER_SIZE",
      lexical_cast<string>(RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE), &options, NULL));
  EXPECT_EQ(RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE, options.runtime_bloom_filter_size);

  // Parsing memory values works in a reasonable way:
  EXPECT_OK(SetQueryOption("RUNTIME_BLOOM_FILTER_SIZE", "1MB", &options, NULL));
  EXPECT_EQ(1 << 20, options.runtime_bloom_filter_size);

  // Bloom filters cannot occupy a percentage of memory:
  EXPECT_FALSE(SetQueryOption("RUNTIME_BLOOM_FILTER_SIZE", "10%", &options, NULL).ok());
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

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  MemInfo::Init();
  return RUN_ALL_TESTS();
}
