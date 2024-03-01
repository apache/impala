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

#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "testutil/gtest-util.h"

#include "gen-cpp/Types_types.h"
#include "service/query-state-record.h"

namespace impala {

TEST(QueryStateRecordTest, StartTimeComparatorNotEqual) {
  QueryStateRecord::StartTimeComparator fixture;

  QueryStateRecord a;
  QueryStateRecord b;

  a.start_time_us = std::numeric_limits<int64_t>::min();
  b.start_time_us = std::numeric_limits<int64_t>::max();

  EXPECT_TRUE(fixture(a, b));
  EXPECT_FALSE(fixture(b, a));
}

TEST(QueryStateRecordTest, StartTimeComparatorEqualIdSame) {
  QueryStateRecord::StartTimeComparator fixture;

  QueryStateRecord a;
  QueryStateRecord b;

  a.start_time_us = 1;
  b.start_time_us = 1;

  EXPECT_FALSE(fixture(a, b));
  EXPECT_FALSE(fixture(b, a));
}

TEST(QueryStateRecordTest, StartTimeComparatorEqualIdDifferent) {
  QueryStateRecord::StartTimeComparator fixture;

  QueryStateRecord a;
  QueryStateRecord b;

  a.start_time_us = 1;
  a.id.lo = 1;
  a.id.hi = 2;
  b.start_time_us = 1;

  EXPECT_FALSE(fixture(a, b));
  EXPECT_TRUE(fixture(b, a));
}

TEST(QueryStateRecordTest, EventsTimelineIterator) {
  std::vector<std::string> labels;
  std::vector<std::int64_t> timestamps;
  int cntr = 0;

  labels.push_back("three");
  timestamps.push_back(3);

  labels.push_back("four");
  timestamps.push_back(4);

  labels.push_back("zero");
  timestamps.push_back(0);

  labels.push_back("two");
  timestamps.push_back(2);

  labels.push_back("one");
  timestamps.push_back(1);

  labels.push_back("one");
  timestamps.push_back(1);

  for (const auto& actual : EventsTimelineIterator(&labels, &timestamps)) {
    switch (cntr) {
    case 0:
      EXPECT_EQ("three", actual.first);
      EXPECT_EQ(3, actual.second);
      break;
    case 1:
      EXPECT_EQ("four", actual.first);
      EXPECT_EQ(4, actual.second);
      break;
    case 2:
      EXPECT_EQ("zero", actual.first);
      EXPECT_EQ(0, actual.second);
      break;
    case 3:
      EXPECT_EQ("two", actual.first);
      EXPECT_EQ(2, actual.second);
      break;
    case 4:
      EXPECT_EQ("one", actual.first);
      EXPECT_EQ(1, actual.second);
      break;
    case 5:
      EXPECT_EQ("one", actual.first);
      EXPECT_EQ(1, actual.second);
      break;
    default:
      FAIL();
    }

    cntr++;
  }
}

TEST(QueryStateRecordTest, PerHostStatePeakMemoryComparatorLessThan) {
  TNetworkAddress addr_a;
  PerHostState a;
  a.peak_memory_usage = std::numeric_limits<int64_t>::min();
  std::pair<TNetworkAddress, PerHostState> pair_a = std::make_pair(addr_a, a);

  TNetworkAddress addr_b;
  PerHostState b;
  b.peak_memory_usage = std::numeric_limits<int64_t>::max();
  std::pair<TNetworkAddress, PerHostState> pair_b = std::make_pair(addr_b, b);

  EXPECT_TRUE(PerHostPeakMemoryComparator(pair_a, pair_b));
  EXPECT_FALSE(PerHostPeakMemoryComparator(pair_b, pair_a));
}

TEST(QueryStateRecordTest, PerHostStatePeakMemoryComparatorEqual) {
  TNetworkAddress addr_a;
  PerHostState a;
  std::pair<TNetworkAddress, PerHostState> pair_a = std::make_pair(addr_a, a);

  TNetworkAddress addr_b;
  PerHostState b;
  std::pair<TNetworkAddress, PerHostState> pair_b = std::make_pair(addr_b, b);

  EXPECT_FALSE(PerHostPeakMemoryComparator(pair_a, pair_b));
  EXPECT_FALSE(PerHostPeakMemoryComparator(pair_b, pair_a));
}

} //namespace impala
