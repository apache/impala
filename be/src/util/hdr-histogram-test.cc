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
#include "util/hdr-histogram.h"
#include "testutil/gtest-util.h"

namespace impala {

class HdrHistogramTest : public testing::Test {};

TEST_F(HdrHistogramTest, HistogramTotalSum) {
  constexpr uint64_t MAX_VALUE = 10000;
  HdrHistogram* histogram = new HdrHistogram(MAX_VALUE, 3);

  histogram->Increment(100);
  histogram->IncrementBy(10, 50);
  EXPECT_EQ(histogram->TotalSum(), 600);

  histogram->Increment(24000);
  EXPECT_EQ(histogram->TotalSum(), 24600);
}

TEST_F(HdrHistogramTest, HistogramTotalSumIsMovedToNewHdrHistogram) {
  constexpr uint64_t MAX_VALUE = 10000;
  HdrHistogram* histogram = new HdrHistogram(MAX_VALUE, 3);

  histogram->Increment(100);
  EXPECT_EQ(histogram->TotalSum(), 100);

  HdrHistogram* new_histogram = new HdrHistogram(*histogram);
  EXPECT_EQ(new_histogram->TotalSum(), 100);

  new_histogram->Increment(200);
  EXPECT_EQ(new_histogram->TotalSum(), 300);
}
} // namespace impala
