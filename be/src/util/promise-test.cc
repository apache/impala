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

#include "runtime/timestamp-value.h"
#include "testutil/gtest-util.h"
#include "testutil/death-test-util.h"
#include "util/promise.h"
#include "util/time.h"

#include "common/names.h"

namespace impala {

void RunThread(Promise<int64_t>* promise) {
  promise->Set(100);
}

TEST(PromiseTest, BasicTest) {
  Promise<int64_t> promise;
  thread promise_setter(RunThread, &promise);

  ASSERT_EQ(promise.Get(), 100);
}

TEST(PromiseTest, TimeoutTest) {
  // Test that the promise can be fulfilled by setting a value.
  bool timed_out = true;
  Promise<int64_t> fulfilled_promise;
  thread promise_setter(RunThread, &fulfilled_promise);
  ASSERT_EQ(fulfilled_promise.Get(10000, &timed_out), 100);
  ASSERT_EQ(timed_out, false);

  // Test that the promise times out properly.
  int64_t start_time, end_time;
  start_time = MonotonicMillis();
  timed_out = false;
  Promise<int64_t> timedout_promise;
  timedout_promise.Get(1000, &timed_out);
  ASSERT_EQ(timed_out, true);
  end_time = MonotonicMillis();
  ASSERT_GE(end_time - start_time, 1000);
}

TEST(PromiseDeathTest, RepeatedSetTest) {
  // Hint to gtest that only one thread is being used here. Multiple threads are unsafe
  // for 'death' tests, see
  // https://code.google.com/p/googletest/wiki/AdvancedGuide#Death_Tests for more detail
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  Promise<int64_t> promise;
  promise.Set(100);
  IMPALA_ASSERT_DEBUG_DEATH(
      promise.Set(150), "Called Set\\(\\.\\.\\) twice on the same Promise");
}

TEST(PromiseTest, RepeatedTestInMultipleProducerMode) {
  Promise<int64_t, PromiseMode::MULTIPLE_PRODUCER> promise;
  ASSERT_EQ(promise.Set(100), 100);
  ASSERT_EQ(promise.Set(200), 100);
  ASSERT_EQ(promise.Get(), 100);
}

TEST(PromiseTest, BasicTestInMultipleProducerMode) {
  Promise<int64_t, PromiseMode::MULTIPLE_PRODUCER> promise;
  thread promise_setter([&promise]() { promise.Set(100); });
  ASSERT_EQ(promise.Get(), 100);
}
}

int main(int argc, char **argv) {
  // TODO: This test does not log into the logs/be_tests directory, but using the
  // standard via InitCommonRuntime() causes the test to fail.
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
