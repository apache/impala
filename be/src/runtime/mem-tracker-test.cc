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
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  // Clean up.
  t.Release(5);
}

TEST(MemTestTest, SingleTrackerWithLimit) {
  MemTracker t(11);
  EXPECT_TRUE(t.has_limit());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 20);
  EXPECT_TRUE(t.LimitExceeded(MemLimit::HARD));
  t.Release(15);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  // Clean up.
  t.Release(5);
}

/// Exercise individual functions that take MemLimit::SOFT option.
TEST(MemTestTest, SoftLimit) {
  MemTracker t(100);
  MemTracker child(-1, "", &t);

  // Exercise functions that return limits.
  EXPECT_EQ(90, t.soft_limit());
  EXPECT_EQ(90, t.GetLimit(MemLimit::SOFT));
  EXPECT_EQ(100, t.GetLimit(MemLimit::HARD));
  EXPECT_EQ(-1, child.soft_limit());
  EXPECT_EQ(90, t.GetLowestLimit(MemLimit::SOFT));
  EXPECT_EQ(90, child.GetLowestLimit(MemLimit::SOFT));
  EXPECT_EQ(100, t.GetLowestLimit(MemLimit::HARD));

  // Test SpareCapacity()
  EXPECT_EQ(100, t.SpareCapacity(MemLimit::HARD));
  EXPECT_EQ(90, t.SpareCapacity(MemLimit::SOFT));
  EXPECT_EQ(100, child.SpareCapacity(MemLimit::HARD));
  EXPECT_EQ(90, child.SpareCapacity(MemLimit::SOFT));

  // Test TryConsume() within soft limit.
  EXPECT_TRUE(t.TryConsume(90, MemLimit::SOFT));
  EXPECT_FALSE(t.LimitExceeded(MemLimit::SOFT));
  EXPECT_FALSE(child.AnyLimitExceeded(MemLimit::SOFT));

  // Test TryConsume() going over soft limit.
  EXPECT_FALSE(t.TryConsume(1, MemLimit::SOFT));
  EXPECT_FALSE(child.TryConsume(1, MemLimit::SOFT));
  EXPECT_TRUE(t.TryConsume(1, MemLimit::HARD));
  EXPECT_TRUE(t.LimitExceeded(MemLimit::SOFT));
  EXPECT_FALSE(child.LimitExceeded(MemLimit::SOFT));
  EXPECT_TRUE(t.AnyLimitExceeded(MemLimit::SOFT));
  EXPECT_TRUE(child.AnyLimitExceeded(MemLimit::SOFT));
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_FALSE(child.AnyLimitExceeded(MemLimit::HARD));
  EXPECT_EQ(9, child.SpareCapacity(MemLimit::HARD));
  EXPECT_EQ(-1, child.SpareCapacity(MemLimit::SOFT));

  // Test Consume() going over hard limit.
  child.Consume(10);
  EXPECT_TRUE(t.LimitExceeded(MemLimit::SOFT));
  EXPECT_TRUE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_FALSE(child.LimitExceeded(MemLimit::SOFT));
  EXPECT_FALSE(child.LimitExceeded(MemLimit::HARD));
  EXPECT_TRUE(t.AnyLimitExceeded(MemLimit::SOFT));
  EXPECT_TRUE(t.AnyLimitExceeded(MemLimit::HARD));
  EXPECT_TRUE(child.AnyLimitExceeded(MemLimit::SOFT));
  EXPECT_TRUE(child.AnyLimitExceeded(MemLimit::HARD));
  EXPECT_EQ(-1, child.SpareCapacity(MemLimit::HARD));
  EXPECT_EQ(-11, child.SpareCapacity(MemLimit::SOFT));

  t.Release(91);
  child.Release(10);
}

TEST(MemTestTest, ConsumptionMetric) {
  TMetricDef md;
  md.__set_key("test");
  md.__set_units(TUnit::BYTES);
  md.__set_kind(TMetricKind::GAUGE);
  IntGauge metric(md, 0);
  EXPECT_EQ(metric.GetValue(), 0);

  TMetricDef neg_md;
  neg_md.__set_key("neg_test");
  neg_md.__set_units(TUnit::BYTES);
  neg_md.__set_kind(TMetricKind::GAUGE);
  NegatedGauge neg_metric(neg_md, &metric);

  MemTracker t(&metric, 100, "");
  MemTracker neg_t(&neg_metric, 100, "");
  EXPECT_TRUE(t.has_limit());
  EXPECT_EQ(t.consumption(), 0);
  EXPECT_EQ(neg_t.consumption(), 0);

  // Consume()/Release() arguments have no effect
  t.Consume(150);
  EXPECT_EQ(t.consumption(), 0);
  EXPECT_EQ(t.peak_consumption(), 0);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(neg_t.consumption(), 0);
  t.Release(5);
  EXPECT_EQ(t.consumption(), 0);
  EXPECT_EQ(t.peak_consumption(), 0);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
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
  t.Release(1);
  neg_t.Consume(1);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_EQ(t.peak_consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(neg_t.consumption(), -5);
  metric.Increment(150);
  t.Consume(1);
  neg_t.Consume(1);
  EXPECT_EQ(t.consumption(), 155);
  EXPECT_EQ(t.peak_consumption(), 155);
  EXPECT_TRUE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(neg_t.consumption(), -155);
  metric.Increment(-150);
  t.Release(1);
  neg_t.Consume(1);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_EQ(t.peak_consumption(), 155);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(neg_t.consumption(), -5);
  // consumption_ is not updated when Consume()/Release() is called with a zero value
  metric.Increment(10);
  t.Consume(0);
  neg_t.Release(0);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_EQ(t.peak_consumption(), 155);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(neg_t.consumption(), -5);
  // consumption_ is not updated when TryConsume() is called with a zero value
  EXPECT_TRUE(t.TryConsume(0));
  EXPECT_TRUE(neg_t.TryConsume(0));
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_EQ(t.peak_consumption(), 155);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(neg_t.consumption(), -5);
  // Clean up.
  metric.Increment(-15);
  t.Release(1);
  neg_t.Release(1);
}

TEST(MemTestTest, TrackerHierarchy) {
  MemTracker p(100);
  MemTracker c1(80, "", &p);
  MemTracker c2(50, "", &p);

  // everything below limits
  c1.Consume(60);
  EXPECT_EQ(c1.consumption(), 60);
  EXPECT_FALSE(c1.LimitExceeded(MemLimit::HARD));
  EXPECT_FALSE(c1.AnyLimitExceeded(MemLimit::HARD));
  EXPECT_EQ(c2.consumption(), 0);
  EXPECT_FALSE(c2.LimitExceeded(MemLimit::HARD));
  EXPECT_FALSE(c2.AnyLimitExceeded(MemLimit::HARD));
  EXPECT_EQ(p.consumption(), 60);
  EXPECT_FALSE(p.LimitExceeded(MemLimit::HARD));
  EXPECT_FALSE(p.AnyLimitExceeded(MemLimit::HARD));

  // p goes over limit
  c2.Consume(50);
  EXPECT_EQ(c1.consumption(), 60);
  EXPECT_FALSE(c1.LimitExceeded(MemLimit::HARD));
  EXPECT_TRUE(c1.AnyLimitExceeded(MemLimit::HARD));
  EXPECT_EQ(c2.consumption(), 50);
  EXPECT_FALSE(c2.LimitExceeded(MemLimit::HARD));
  EXPECT_TRUE(c2.AnyLimitExceeded(MemLimit::HARD));
  EXPECT_EQ(p.consumption(), 110);
  EXPECT_TRUE(p.LimitExceeded(MemLimit::HARD));

  // c2 goes over limit, p drops below limit
  c1.Release(20);
  c2.Consume(10);
  EXPECT_EQ(c1.consumption(), 40);
  EXPECT_FALSE(c1.LimitExceeded(MemLimit::HARD));
  EXPECT_FALSE(c1.AnyLimitExceeded(MemLimit::HARD));
  EXPECT_EQ(c2.consumption(), 60);
  EXPECT_TRUE(c2.LimitExceeded(MemLimit::HARD));
  EXPECT_TRUE(c2.AnyLimitExceeded(MemLimit::HARD));
  EXPECT_EQ(p.consumption(), 100);
  EXPECT_FALSE(p.LimitExceeded(MemLimit::HARD));

  // Clean up.
  c1.Release(40);
  c2.Release(60);
}

// Test that we can transfer between MemTrackers without temporary double-counting
// in ancestors
TEST(MemTestTest, TransferTo) {
  MemTracker root(100);
  MemTracker parent(-1, "", &root);
  MemTracker uncle(-1, "", &root);
  MemTracker child1(-1, "", &parent);
  MemTracker child2(-1, "", &parent);

  child1.Consume(100);
  // To self.
  child1.TransferTo(&child1, 100);
  EXPECT_EQ(child1.consumption(), 100);
  EXPECT_EQ(child1.peak_consumption(), 100);
  EXPECT_EQ(parent.consumption(), 100);
  EXPECT_EQ(parent.peak_consumption(), 100);

  // Child to parent.
  child1.TransferTo(&parent, 100);
  EXPECT_EQ(child1.consumption(), 0);
  EXPECT_EQ(child1.peak_consumption(), 100);
  EXPECT_EQ(parent.consumption(), 100);
  EXPECT_EQ(parent.peak_consumption(), 100);

  // Parent to child
  parent.TransferTo(&child1, 100);
  EXPECT_EQ(child1.consumption(), 100);
  EXPECT_EQ(child1.peak_consumption(), 100);
  EXPECT_EQ(parent.consumption(), 100);
  EXPECT_EQ(parent.peak_consumption(), 100);

  // Child to child.
  child1.TransferTo(&child2, 50);
  EXPECT_EQ(child1.consumption(), 50);
  EXPECT_EQ(child2.consumption(), 50);
  EXPECT_EQ(child2.peak_consumption(), 50);
  EXPECT_EQ(parent.consumption(), 100);
  EXPECT_EQ(parent.peak_consumption(), 100);

  // Child to uncle.
  child1.TransferTo(&uncle, 50);
  EXPECT_EQ(child1.consumption(), 0);
  EXPECT_EQ(uncle.consumption(), 50);
  EXPECT_EQ(uncle.peak_consumption(), 50);
  EXPECT_EQ(parent.consumption(), 50);
  EXPECT_EQ(parent.peak_consumption(), 100);
  EXPECT_EQ(root.consumption(), 100);
  EXPECT_EQ(root.peak_consumption(), 100);

  // Child to root
  child2.TransferTo(&root, 50);
  EXPECT_EQ(child2.consumption(), 0);
  EXPECT_EQ(parent.consumption(), 0);
  EXPECT_EQ(parent.peak_consumption(), 100);
  EXPECT_EQ(root.consumption(), 100);
  EXPECT_EQ(root.peak_consumption(), 100);

  uncle.Release(50);
  root.Release(50);
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
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));

  // Test TryConsume()
  EXPECT_FALSE(t.TryConsume(2));
  EXPECT_EQ(t.consumption(), 9);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));

  // Attach GcFunction that releases 1 byte
  GcFunctionHelper gc_func_helper(&t);
  t.AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper));
  EXPECT_TRUE(t.TryConsume(2));
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));

  // GcFunction will be called even though TryConsume() fails
  EXPECT_FALSE(t.TryConsume(2));
  EXPECT_EQ(t.consumption(), 9);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));

  // GcFunction won't be called
  EXPECT_TRUE(t.TryConsume(1));
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));

  // Test LimitExceeded(MemLimit::HARD)
  t.Consume(1);
  EXPECT_EQ(t.consumption(), 11);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(t.consumption(), 10);

  // Add more GcFunctions, test that we only call them until the limit is no longer
  // exceeded
  GcFunctionHelper gc_func_helper2(&t);
  t.AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper2));
  GcFunctionHelper gc_func_helper3(&t);
  t.AddGcFunction(boost::bind(&GcFunctionHelper::GcFunc, &gc_func_helper3));
  t.Consume(1);
  EXPECT_EQ(t.consumption(), 11);
  EXPECT_FALSE(t.LimitExceeded(MemLimit::HARD));
  EXPECT_EQ(t.consumption(), 10);

  // Clean up.
  t.Release(10);
}

// Test that we can compute topN queries from a hierarchy of mem trackers. These
// queries are represented by 100 query mem trackers.
TEST(MemTestTest, TopN) {
  MemTracker root;
  root.Consume(10);

  static const int NUM_QUERY_MEM_TRACKERS = 100;
  // Populate these many query mem trackers with some memory consumptions.
  std::vector<MemTracker*> trackers;
  for (int i = 0; i < NUM_QUERY_MEM_TRACKERS; i++) {
    MemTracker* tracker = new MemTracker(-1, "", &root);
    tracker->query_id_.hi = 0;
    tracker->query_id_.lo = i;
    tracker->Consume(int64_t(i + 1));
    tracker->is_query_mem_tracker_ = true;
    trackers.push_back(tracker);
  }
  // Ready to compute top 5 queries which should be the last 5 of those
  // populated above. The result is to be saved in pool_stats.heavy_memory_queries.
  TPoolStats pool_stats;
  root.UpdatePoolStatsForQueries(5, pool_stats);
  // Validate the top entries
  for (int i = 0; i < 5; i++) {
    EXPECT_EQ(pool_stats.heavy_memory_queries[i].queryId.hi, 0);
    EXPECT_EQ(
        pool_stats.heavy_memory_queries[i].queryId.lo, NUM_QUERY_MEM_TRACKERS - i - 1);
    EXPECT_EQ(
        pool_stats.heavy_memory_queries[i].memory_consumed, NUM_QUERY_MEM_TRACKERS - i);
  }
  // Delete the allocated query mem trackers.
  for (int i = 0; i < NUM_QUERY_MEM_TRACKERS; i++) {
    trackers[i]->Release(int64_t(i + 1));
    delete trackers[i];
  }
  root.Release(10);
}
}

