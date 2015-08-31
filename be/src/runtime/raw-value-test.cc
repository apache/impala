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

#include <gtest/gtest.h>
#include <sstream>
#include "runtime/raw-value.h"

#include "common/names.h"

using namespace impala;

namespace impala {

class RawValueTest : public testing::Test {
};

TEST_F(RawValueTest, Compare) {
  int64_t v1, v2;
  v1 = -2128609280;
  v2 = 9223372036854775807;
  EXPECT_LT(RawValue::Compare(&v1, &v2, TYPE_BIGINT), 0);
  EXPECT_GT(RawValue::Compare(&v2, &v1, TYPE_BIGINT), 0);

  int32_t i1, i2;
  i1 = 2147483647;
  i2 = -2147483640;
  EXPECT_GT(RawValue::Compare(&i1, &i2, TYPE_INT), 0);
  EXPECT_LT(RawValue::Compare(&i2, &i1, TYPE_INT), 0);

  int16_t s1, s2;
  s1 = 32767;
  s2 = -32767;
  EXPECT_GT(RawValue::Compare(&s1, &s2, TYPE_SMALLINT), 0);
  EXPECT_LT(RawValue::Compare(&s2, &s1, TYPE_SMALLINT), 0);
}

TEST_F(RawValueTest, TypeChar) {
  const int N = 5;
  const char* v1 = "aaaaa";
  const char* v2 = "aaaaab";
  const char* v3 = "aaaab";

  EXPECT_EQ(RawValue::Compare(v1, v1, ColumnType::CreateCharType(N)), 0);
  EXPECT_EQ(RawValue::Compare(v1, v2, ColumnType::CreateCharType(N)), 0);
  EXPECT_LT(RawValue::Compare(v1, v3, ColumnType::CreateCharType(N)), 0);

  EXPECT_EQ(RawValue::Compare(v2, v1, ColumnType::CreateCharType(N)), 0);
  EXPECT_EQ(RawValue::Compare(v2, v2, ColumnType::CreateCharType(N)), 0);
  EXPECT_LT(RawValue::Compare(v2, v3, ColumnType::CreateCharType(N)), 0);

  EXPECT_GT(RawValue::Compare(v3, v1, ColumnType::CreateCharType(N)), 0);
  EXPECT_GT(RawValue::Compare(v3, v2, ColumnType::CreateCharType(N)), 0);
  EXPECT_EQ(RawValue::Compare(v3, v3, ColumnType::CreateCharType(N)), 0);

  // Try putting non-string data (multiple nulls, non-ascii) and make
  // sure we can output it.
  stringstream ss;
  int val = 123;
  RawValue::PrintValue(&val, ColumnType::CreateCharType(sizeof(int)), -1, &ss);
  string s = ss.str();
  EXPECT_EQ(s.size(), sizeof(int));
  EXPECT_EQ(memcmp(&val, s.data(), sizeof(int)), 0);
}

// IMPALA-2270: "", false, and NULL should hash to distinct values.
TEST_F(RawValueTest, HashEmptyAndNull) {
  uint32_t seed = 12345;
  uint32_t null_hash = RawValue::GetHashValue(NULL, TYPE_STRING, seed);
  uint32_t null_hash_fnv = RawValue::GetHashValueFnv(NULL, TYPE_STRING, seed);
  StringValue empty(NULL, 0);
  uint32_t empty_hash = RawValue::GetHashValue(&empty, TYPE_STRING, seed);
  uint32_t empty_hash_fnv = RawValue::GetHashValueFnv(&empty, TYPE_STRING, seed);
  bool false_val = false;
  uint32_t false_hash = RawValue::GetHashValue(&false_val, TYPE_BOOLEAN, seed);
  uint32_t false_hash_fnv = RawValue::GetHashValue(&false_val, TYPE_BOOLEAN, seed);
  EXPECT_NE(seed, null_hash);
  EXPECT_NE(seed, empty_hash);
  EXPECT_NE(seed, false_hash);
  EXPECT_NE(seed, null_hash_fnv);
  EXPECT_NE(seed, empty_hash_fnv);
  EXPECT_NE(seed, false_hash_fnv);
  EXPECT_NE(null_hash, empty_hash);
  EXPECT_NE(null_hash_fnv, empty_hash_fnv);
  EXPECT_NE(null_hash, false_hash);
  EXPECT_NE(false_hash, null_hash_fnv);
}

/// IMPALA-2270: Test that FNV hash of (int, "") is not skewed.
TEST(HashUtil, IntNullSkew) {
  int num_values = 100000;
  int num_buckets = 16;
  int buckets[num_buckets];
  memset(buckets, 0, sizeof(buckets));
  for (int32_t i = 0; i < num_values; ++i) {
    uint32_t hash = RawValue::GetHashValueFnv(&i, TYPE_INT, 9999);
    StringValue empty(NULL, 0);
    hash = RawValue::GetHashValueFnv(&empty, TYPE_STRING, hash);
    ++buckets[hash % num_buckets];
  }

  for (int i = 0; i < num_buckets; ++i) {
    LOG(INFO) << "Bucket " << i << ": " << buckets[i];
    double exp_count = num_values / (double) num_buckets;
    EXPECT_GT(buckets[i], 0.9 * exp_count) << "Bucket " << i
                                           << " has <= 90%% of expected";
    EXPECT_LT(buckets[i], 1.1 * exp_count) << "Bucket " << i
                                           << " has >= 110%% of expected";
  }
}

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}
