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

#include <string>
#include <boost/bind.hpp>

#include "runtime/mem-tracker.h"
#include "testutil/gtest-util.h"
#include "util/metrics.h"

#include "common/names.h"

namespace impala {

TEST(MemTestTest, SingleTrackerNoLimit) {
  MemTracker t;
  EXPECT_FALSE(t.has_limit());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 10);
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 20);
  t.Release(15);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_FALSE(t.LimitExceeded());
  // Clean up.
  t.Release(5);
}

TEST(MemTestTest, SingleTrackerWithLimit) {
  MemTracker t(11);
  EXPECT_TRUE(t.has_limit());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 20);
  EXPECT_TRUE(t.LimitExceeded());
  t.Release(15);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_FALSE(t.LimitExceeded());
  // Clean up.
  t.Release(5);
}

TEST(MemTestTest, ConsumptionMetric) {
  TMetricDef md;
  md.__set_key("test");
  md.__set_units(TUnit::BYTES);
  md.__set_kind(TMetricKind::GAUGE);
  IntGauge metric(md, 0);
  EXPECT_EQ(metric.value(), 0);

  TMetricDef neg_md;
  neg_md.__set_key("neg_test");
  neg_md.__set_units(TUnit::BYTES);
  neg_md.__set_kind(TMetricKind::GAUGE);
  NegatedGauge<int64_t> neg_metric(neg_md, &metric);

  MemTracker t(&metric, 100, "");
  MemTracker neg_t(&neg_metric, 100, "");
  EXPECT_TRUE(t.has_limit());
  EXPECT_EQ(t.consumption(), 0);
  EXPECT_EQ(neg_t.consumption(), 0);

  // Consume()/Release() arguments have no effect
  t.Consume(150);
  EXPECT_EQ(t.consumption(), 0);
  EXPECT_EQ(t.peak_consumption(), 0);
  EXPECT_FALSE(t.LimitExceeded());
  EXPECT_EQ(neg_t.consumption(), 0);
  t.Release(5);
  EXPECT_EQ(t.consumption(), 0);
  EXPECT_EQ(t.peak_consumption(), 0);
  EXPECT_FALSE(t.LimitExceeded());
  EXPECT_EQ(neg_t.consumption(), 0);

  metric.Increment(10);
  // consumption_ is only updated with consumption_metric_ after calls to
  // Consume()/Release() with a non-zero value
  t.Consume(1);
  neg_t.Consume(1);
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_EQ(t.peak_consumption(), 10);
  EXPECT_EQ(neg_t.consumption(), -10);
  metric.Increment(-5);
  t.Consume(-1);
  neg_t.Consume(1);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_EQ(t.peak_consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded());
  EXPECT_EQ(neg_t.consumption(), -5);
  metric.Increment(150);
  t.Consume(1);
  neg_t.Consume(1);
  EXPECT_EQ(t.consumption(), 155);
  EXPECT_EQ(t.peak_consumption(), 155);
  EXPECT_TRUE(t.LimitExceeded());
  EXPECT_EQ(neg_t.consumption(), -155);
  metric.Increment(-150);
  t.Consume(-1);
  neg_t.Consume(1);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_EQ(t.peak_consumption(), 155);
  EXPECT_FALSE(t.LimitExceeded());
  EXPECT_EQ(neg_t.consumption(), -5);
  // consumption_ is not updated when Consume()/Release() is called with a zero value
  metric.Increment(10);
  t.Consume(0);
  neg_t.Consume(0);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_EQ(t.peak_consumption(), 155);
  EXPECT_FALSE(t.LimitExceeded());
  EXPECT_EQ(neg_t.consumption(), -5);
  // Clean up.
  metric.Increment(-15);
  t.Consume(-1);
  neg_t.Consume(-1);
}

TEST(MemTestTest, TrackerHierarchy) {
  MemTracker p(100);
  MemTracker c1(80, "", &p);
  MemTracker c2(50, "", &p);

  // everything below limits
  c1.Consume(60);
  EXPECT_EQ(c1.consumption(), 60);
  EXPECT_FALSE(c1.LimitExceeded());
  EXPECT_FALSE(c1.AnyLimitExceeded());
  EXPECT_EQ(c2.consumption(), 0);
  EXPECT_FALSE(c2.LimitExceeded());
  EXPECT_FALSE(c2.AnyLimitExceeded());
  EXPECT_EQ(p.consumption(), 60);
  EXPECT_FALSE(p.LimitExceeded());
  EXPECT_FALSE(p.AnyLimitExceeded());

  // p goes over limit
  c2.Consume(50);
  EXPECT_EQ(c1.consumption(), 60);
  EXPECT_FALSE(c1.LimitExceeded());
  EXPECT_TRUE(c1.AnyLimitExceeded());
  EXPECT_EQ(c2.consumption(), 50);
  EXPECT_FALSE(c2.LimitExceeded());
  EXPECT_TRUE(c2.AnyLimitExceeded());
  EXPECT_EQ(p.consumption(), 110);
  EXPECT_TRUE(p.LimitExceeded());

  // c2 goes over limit, p drops below limit
  c1.Release(20);
  c2.Consume(10);
  EXPECT_EQ(c1.consumption(), 40);
  EXPECT_FALSE(c1.LimitExceeded());
  EXPECT_FALSE(c1.AnyLimitExceeded());
  EXPECT_EQ(c2.consumption(), 60);
  EXPECT_TRUE(c2.LimitExceeded());
  EXPECT_TRUE(c2.AnyLimitExceeded());
  EXPECT_EQ(p.consumption(), 100);
  EXPECT_FALSE(p.LimitExceeded());

  // Clean up.
  c1.Release(40);
  c2.Release(60);
}

class GcFunctionHelper {
 public:
  static const int NUM_RELEASE_BYTES = 1;

  GcFunctionHelper(MemTracker* tracker) : tracker_(tracker) { }

  void GcFunc() { tracker_->Release(NUM_RELEASE_BYTES); }

 private:
  MemTracker* tracker_;
};

TEST(MemTestTest, GcFunctions) {
  MemTracker t(10);
  ASSERT_TRUE(t.has_limit());

  t.Consume(9);
  EXPECT_FALSE(t.LimitExceeded());

  // Test TryConsume()
  EXPECT_FALSE(t.TryConsume(2));
  EXPECT_EQ(t.consumption(), 9);
  EXPECT_FALSE(t.LimitExceeded());

  // Attach GcFunction that releases 1 byte
  GcFunctionHelper gc_func_helper(&t);
  t.AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper));
  EXPECT_TRUE(t.TryConsume(2));
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded());

  // GcFunction will be called even though TryConsume() fails
  EXPECT_FALSE(t.TryConsume(2));
  EXPECT_EQ(t.consumption(), 9);
  EXPECT_FALSE(t.LimitExceeded());

  // GcFunction won't be called
  EXPECT_TRUE(t.TryConsume(1));
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded());

  // Test LimitExceeded()
  t.Consume(1);
  EXPECT_EQ(t.consumption(), 11);
  EXPECT_FALSE(t.LimitExceeded());
  EXPECT_EQ(t.consumption(), 10);

  // Add more GcFunctions, test that we only call them until the limit is no longer
  // exceeded
  GcFunctionHelper gc_func_helper2(&t);
  t.AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper2));
  GcFunctionHelper gc_func_helper3(&t);
  t.AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper3));
  t.Consume(1);
  EXPECT_EQ(t.consumption(), 11);
  EXPECT_FALSE(t.LimitExceeded());
  EXPECT_EQ(t.consumption(), 10);

  //Clean up.
  t.Release(10);
}

}

IMPALA_TEST_MAIN();
