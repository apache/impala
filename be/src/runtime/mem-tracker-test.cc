// Copyright 2013 Cloudera Inc.
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

#include <string>
#include <gtest/gtest.h>

#include "runtime/mem-tracker.h"

using namespace std;

namespace impala {

TEST(MemTestTest, SingleTrackerNoLimit) {
  MemTracker t;
  EXPECT_FALSE(t.has_limit());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_EQ(t.peak_consumption(), 10);
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 20);
  EXPECT_EQ(t.peak_consumption(), 20);
  t.Release(15);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_EQ(t.peak_consumption(), 20);
  EXPECT_FALSE(t.LimitExceeded());
}

TEST(MemTestTest, SingleTrackerWithLimit) {
  MemTracker t(11);
  EXPECT_TRUE(t.has_limit());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 10);
  EXPECT_EQ(t.peak_consumption(), 10);
  EXPECT_FALSE(t.LimitExceeded());
  t.Consume(10);
  EXPECT_EQ(t.consumption(), 20);
  EXPECT_EQ(t.peak_consumption(), 20);
  EXPECT_TRUE(t.LimitExceeded());
  t.Release(15);
  EXPECT_EQ(t.consumption(), 5);
  EXPECT_EQ(t.peak_consumption(), 20);
  EXPECT_FALSE(t.LimitExceeded());
}

TEST(MemTestTest, TrackerHierarchy) {
  MemTracker p(100);
  MemTracker c1(80, "", &p);
  MemTracker c2(50, "", &p);

  // everything below limits
  c1.Consume(60);
  EXPECT_EQ(c1.consumption(), 60);
  EXPECT_EQ(c1.peak_consumption(), 60);
  EXPECT_FALSE(c1.LimitExceeded());
  EXPECT_FALSE(c1.AnyLimitExceeded());
  EXPECT_EQ(c2.consumption(), 0);
  EXPECT_EQ(c2.peak_consumption(), 0);
  EXPECT_FALSE(c2.LimitExceeded());
  EXPECT_FALSE(c2.AnyLimitExceeded());
  EXPECT_EQ(p.consumption(), 60);
  EXPECT_EQ(p.peak_consumption(), 60);
  EXPECT_FALSE(p.LimitExceeded());
  EXPECT_FALSE(p.AnyLimitExceeded());

  // p goes over limit
  c2.Consume(50);
  EXPECT_EQ(c1.consumption(), 60);
  EXPECT_EQ(c1.peak_consumption(), 60);
  EXPECT_FALSE(c1.LimitExceeded());
  EXPECT_TRUE(c1.AnyLimitExceeded());
  EXPECT_EQ(c2.consumption(), 50);
  EXPECT_EQ(c2.peak_consumption(), 50);
  EXPECT_FALSE(c2.LimitExceeded());
  EXPECT_TRUE(c2.AnyLimitExceeded());
  EXPECT_EQ(p.consumption(), 110);
  EXPECT_EQ(p.peak_consumption(), 110);
  EXPECT_TRUE(p.LimitExceeded());
 
  // c2 goes over limit, p drops below limit
  c1.Release(20);
  c2.Consume(10);
  EXPECT_EQ(c1.consumption(), 40);
  EXPECT_EQ(c1.peak_consumption(), 60);
  EXPECT_FALSE(c1.LimitExceeded());
  EXPECT_FALSE(c1.AnyLimitExceeded());
  EXPECT_EQ(c2.consumption(), 60);
  EXPECT_EQ(c2.peak_consumption(), 60);
  EXPECT_TRUE(c2.LimitExceeded());
  EXPECT_TRUE(c2.AnyLimitExceeded());
  EXPECT_EQ(p.consumption(), 100);
  EXPECT_EQ(p.peak_consumption(), 110);
  EXPECT_FALSE(p.LimitExceeded()); 
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

