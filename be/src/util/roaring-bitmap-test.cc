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
#include <unordered_set>

#include "testutil/gtest-util.h"
#include "util/roaring-bitmap.h"

#include "common/names.h"

using namespace std;

namespace impala {

TEST(RoaringBitmap64Test, Empty) {
  RoaringBitmap64 rbm;
  RoaringBitmap64::BulkContext context;
  ASSERT_TRUE(rbm.IsEmpty());
  ASSERT_EQ(rbm.Max(), numeric_limits<uint64_t>::min());
  ASSERT_EQ(rbm.Min(), numeric_limits<uint64_t>::max());
  RoaringBitmap64::Iterator it(rbm);
  ASSERT_EQ(it.GetEqualOrLarger(0), numeric_limits<uint64_t>::max());
  ASSERT_EQ(it.GetEqualOrLarger(1000000), numeric_limits<uint64_t>::max());
}

TEST(RoaringBitmap64Test, Basic) {
  RoaringBitmap64 rbm;
  RoaringBitmap64::BulkContext context;
  rbm.AddElements({11, 29});
  ASSERT_FALSE(rbm.IsEmpty());
  for (int i = 0; i < 100; ++i) {
    if (i == 11 || i == 29) {
      ASSERT_TRUE(rbm.ContainsBulk(i, &context));
    } else {
      ASSERT_FALSE(rbm.ContainsBulk(i, &context));
    }
  }
  ASSERT_EQ(rbm.Max(), 29);
  ASSERT_EQ(rbm.Min(), 11);
}

TEST(RoaringBitmap64Test, AddMany) {
  RoaringBitmap64 rbm;
  RoaringBitmap64::BulkContext context;
  vector<uint64_t> elements_to_add_1({11, 13, 15, 15, 3, 11, 7, 15});
  vector<uint64_t> elements_to_add_2({100, 600000, 100000, 3000000});
  vector<uint64_t> elements_to_add_3({500000, 27, 140000, 2000000,
      25, 65500, 300000, 120000, 2500000, 11, 15, 23, 898989, 300});
  rbm.AddElements(elements_to_add_1);
  rbm.AddElements(elements_to_add_2);
  rbm.AddElements(elements_to_add_3);
  unordered_set<uint64_t> all_elements;
  all_elements.insert(elements_to_add_1.begin(), elements_to_add_1.end());
  all_elements.insert(elements_to_add_2.begin(), elements_to_add_2.end());
  all_elements.insert(elements_to_add_3.begin(), elements_to_add_3.end());
  for (uint64_t i = 0; i <= 3000000; ++i) {
    if (all_elements.find(i) != all_elements.end()) {
      ASSERT_TRUE(rbm.ContainsBulk(i, &context));
    } else {
      ASSERT_FALSE(rbm.ContainsBulk(i, &context));
    }
  }
  RoaringBitmap64::Iterator it(rbm);
  ASSERT_EQ(it.GetEqualOrLarger(0), 3);
  ASSERT_EQ(it.GetEqualOrLarger(3), 3);
  ASSERT_EQ(it.GetEqualOrLarger(13), 13);
  // Querying the same element repeatedly shouldn't be an issue, as
  // the iterator should remain intact.
  ASSERT_EQ(it.GetEqualOrLarger(13), 13);
  ASSERT_EQ(it.GetEqualOrLarger(13), 13);
  ASSERT_EQ(it.GetEqualOrLarger(14), 15);
  ASSERT_EQ(it.GetEqualOrLarger(14), 15);
  ASSERT_EQ(it.GetEqualOrLarger(3000000), 3000000);
  ASSERT_EQ(it.GetEqualOrLarger(3000001), std::numeric_limits<uint64_t>::max());
  ASSERT_EQ(rbm.Max(), 3000000);
  ASSERT_EQ(rbm.Min(), 3);

  // Iterate with larger jumps
  RoaringBitmap64::Iterator jump_it(rbm);
  ASSERT_EQ(jump_it.GetEqualOrLarger(99), 100);
  ASSERT_EQ(jump_it.GetEqualOrLarger(3000001), std::numeric_limits<uint64_t>::max());
}

TEST(RoaringBitmap64Test, DenseIterationOverDenseMap) {
  constexpr int SIZE = 100000;
  RoaringBitmap64 rbm;
  vector<uint64_t> elements_to_add;
  elements_to_add.reserve(SIZE);
  for (int i = 0; i < SIZE; ++i) {
    if (i % 4 != 0) {
      elements_to_add.push_back(i);
    }
  }
  rbm.AddElements(elements_to_add);
  RoaringBitmap64::Iterator it(rbm);
  for (int i = 0; i < SIZE; ++i) {
    if (i % 4 != 0) {
      ASSERT_EQ(it.GetEqualOrLarger(i), i);
    } else {
      ASSERT_EQ(it.GetEqualOrLarger(i), i+1);
    }
  }
}

TEST(RoaringBitmap64Test, SparseIterationOverDenseMap) {
  constexpr int SIZE = 100000;
  RoaringBitmap64 rbm;
  vector<uint64_t> elements_to_add;
  elements_to_add.reserve(SIZE);
  for (int i = 0; i < SIZE; ++i) {
    if (i % 4 != 0) {
      elements_to_add.push_back(i);
    }
  }
  rbm.AddElements(elements_to_add);
  RoaringBitmap64::Iterator it(rbm);
  for (int i = 0; i < SIZE; i += 51) {
    if (i % 4 != 0) {
      ASSERT_EQ(it.GetEqualOrLarger(i), i);
    } else {
      ASSERT_EQ(it.GetEqualOrLarger(i), i+1);
    }
  }
}

TEST(RoaringBitmap64Test, DenseIterationOverSparseMap) {
  constexpr int SIZE = 1000000;
  constexpr int DIV = 97;
  RoaringBitmap64 rbm;
  vector<uint64_t> elements_to_add;
  elements_to_add.reserve(SIZE);
  for (int i = 0; i < SIZE; i += DIV) {
    elements_to_add.push_back(i);
  }
  rbm.AddElements(elements_to_add);
  RoaringBitmap64::Iterator it(rbm);
  for (int i = 0; i < SIZE; ++i) {
    if (i % DIV == 0) {
      ASSERT_EQ(it.GetEqualOrLarger(i), i);
    } else if (i - (i % DIV) + DIV >= SIZE) {
      // The next number divisible by DIV is not in the bitmap.
      ASSERT_EQ(it.GetEqualOrLarger(i), std::numeric_limits<uint64_t>::max());
    } else {
      // This could be used in the first branch, but it's a bit clearer what we check.
      ASSERT_EQ(it.GetEqualOrLarger(i), i - (i % DIV) + DIV);
    }
  }
}

TEST(RoaringBitmap64Test, SparseIterationOverSparseMap) {
  constexpr int SIZE = 1000000;
  constexpr int DIV = 83;
  RoaringBitmap64 rbm;
  vector<uint64_t> elements_to_add;
  elements_to_add.reserve(SIZE);
  for (int i = 0; i < SIZE; i += DIV) {
    elements_to_add.push_back(i);
  }
  rbm.AddElements(elements_to_add);
  RoaringBitmap64::Iterator it(rbm);
  for (int i = 0; i < SIZE; i += 41) {
    if (i % DIV == 0) {
      ASSERT_EQ(it.GetEqualOrLarger(i), i);
    } else if (i - (i % DIV) + DIV >= SIZE) {
      // The next number divisible by DIV is not in the bitmap.
      ASSERT_EQ(it.GetEqualOrLarger(i), std::numeric_limits<uint64_t>::max());
    } else {
      // This could be used in the first branch, but it's a bit clearer what we check.
      ASSERT_EQ(it.GetEqualOrLarger(i), i - (i % DIV) + DIV);
    }
  }
}

} // namespace impala
