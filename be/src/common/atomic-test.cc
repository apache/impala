// Copyright 2012 Cloudera Inc.
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

#include "common/atomic.h"

namespace impala {

// Simple test to make sure there is no obvious error in the usage of the
// __sync* operations.  This is not intended to test the thread safety.
TEST(AtomicTest, Basic) {
  AtomicInt<int> i1;
  EXPECT_EQ(i1, 0);
  i1 = 10;
  EXPECT_EQ(i1, 10);
  i1 += 5;
  EXPECT_EQ(i1, 15);
  i1 -= 25;
  EXPECT_EQ(i1, -10);
  ++i1;
  EXPECT_EQ(i1, -9);
  --i1;
  EXPECT_EQ(i1, -10);
  i1 = 100;
  EXPECT_EQ(i1, 100);

  i1.UpdateMax(50);
  EXPECT_EQ(i1, 100);
  i1.UpdateMax(150);
  EXPECT_EQ(i1, 150);

  i1.UpdateMin(200);
  EXPECT_EQ(i1, 150);
  i1.UpdateMin(-200);
  EXPECT_EQ(i1, -200);
}

TEST(AtomicTest, TestAndSet) {
  AtomicInt<int> i1;
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(i + 1, i1.UpdateAndFetch(1));
  }

  i1 = 0;

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(i, i1.FetchAndUpdate(1));
  }
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
