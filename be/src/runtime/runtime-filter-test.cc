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

#include <boost/thread/thread.hpp>

#include "common/init.h"
#include "common/object-pool.h"
#include "runtime/runtime-filter.h"
#include "runtime/runtime-filter.inline.h"
#include "testutil/gtest-util.h"
#include "util/stopwatch.h"

#include "common/names.h"

using namespace impala;

namespace impala {

class RuntimeFilterTest : public testing::Test {
 protected:
  ObjectPool pool_;
  MemTracker tracker_;

  virtual void SetUp() {}

  virtual void TearDown() { pool_.Clear(); }

  void SetDelay(RuntimeFilter* rf, int64_t delay) { rf->injection_delay_ = delay; }
};

struct TestConfig {
  RuntimeFilter* runtime_filter;
  int64_t injection_delay;
  int64_t wait_for_ms;
  MinMaxFilter* min_max_filter;
};

// Test that RuntimeFilter stop waiting after it is canceled.
// See IMPALA-9612.
TEST_F(RuntimeFilterTest, Canceled) {
  TRuntimeFilterDesc desc;
  desc.__set_type(TRuntimeFilterType::MIN_MAX);
  RuntimeFilter* rf = pool_.Add(new RuntimeFilter(desc, desc.filter_size_bytes));
  TestConfig tc = {rf, 500, 1000, nullptr};

  SetDelay(rf, tc.injection_delay);
  MonotonicStopWatch sw;
  thread_group workers;

  sw.Start();
  workers.add_thread(
      new thread([&tc] { tc.runtime_filter->WaitForArrival(tc.wait_for_ms); }));
  SleepForMs(100); // give waiting thread a head start
  workers.add_thread(new thread([&tc] { tc.runtime_filter->Cancel(); }));
  workers.join_all();
  sw.Stop();

  ASSERT_GE(tc.runtime_filter->arrival_delay_ms(), tc.injection_delay);
  ASSERT_LT(sw.ElapsedTime(), (tc.injection_delay + tc.wait_for_ms) * 1000000);
}

// Test that RuntimeFilter stop waiting after the filter arrived.
// See IMPALA-9612.
TEST_F(RuntimeFilterTest, Arrived) {
  TRuntimeFilterDesc desc;
  desc.__set_type(TRuntimeFilterType::MIN_MAX);
  RuntimeFilter* rf = pool_.Add(new RuntimeFilter(desc, desc.filter_size_bytes));
  MinMaxFilter* mmf =
      MinMaxFilter::Create(ColumnType(PrimitiveType::TYPE_BOOLEAN), &pool_, &tracker_);
  TestConfig tc = {rf, 500, 1000, mmf};

  SetDelay(rf, tc.injection_delay);
  MonotonicStopWatch sw;
  thread_group workers;

  sw.Start();
  workers.add_thread(
      new thread([&tc] { tc.runtime_filter->WaitForArrival(tc.wait_for_ms); }));
  SleepForMs(100); // give waiting thread a head start
  workers.add_thread(
      new thread([&tc] {
        tc.runtime_filter->SetFilter(nullptr, tc.min_max_filter, nullptr);
      }));
  workers.join_all();
  sw.Stop();

  ASSERT_GE(tc.runtime_filter->arrival_delay_ms(), tc.injection_delay);
  ASSERT_LT(sw.ElapsedTime(), (tc.injection_delay + tc.wait_for_ms) * 1000000);
}

} // namespace impala
